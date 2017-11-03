/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.elasticsearch.dao;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.metron.elasticsearch.utils.ElasticsearchUtils;
import org.apache.metron.indexing.dao.AccessConfig;
import org.apache.metron.indexing.dao.IndexDao;
import org.apache.metron.indexing.dao.search.FieldType;
import org.apache.metron.indexing.dao.search.Group;
import org.apache.metron.indexing.dao.search.GroupOrder;
import org.apache.metron.indexing.dao.search.GroupOrderType;
import org.apache.metron.indexing.dao.search.GroupRequest;
import org.apache.metron.indexing.dao.search.GroupResponse;
import org.apache.metron.indexing.dao.search.GroupResult;
import org.apache.metron.indexing.dao.search.InvalidSearchException;
import org.apache.metron.indexing.dao.search.SearchRequest;
import org.apache.metron.indexing.dao.search.SearchResponse;
import org.apache.metron.indexing.dao.search.SearchResult;
import org.apache.metron.indexing.dao.search.SortField;
import org.apache.metron.indexing.dao.search.SortOrder;
import org.apache.metron.indexing.dao.update.Document;
import org.elasticsearch.action.ActionWriteResponse.ShardInfo;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.metron.elasticsearch.utils.ElasticsearchUtils.INDEX_NAME_DELIMITER;

public class ElasticsearchDao implements IndexDao {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The value required to ensure that Elasticsearch sorts missing values last.
   */
  private static final String MISSING_SORT_LAST = "_last";

  /**
   * The value required to ensure that Elasticsearch sorts missing values last.
   */
  private static final String MISSING_SORT_FIRST = "_first";

  private transient TransportClient client;

  /**
   * Retrieves column metadata about search indices.
   */
  private ColumnMetadataDao columnMetadataDao;

  /**
   * Handles the submission of search requests to Elasticsearch.
   */
  private ElasticsearchSearchSubmitter searchSubmitter;

  private AccessConfig accessConfig;
  private List<String> ignoredIndices = new ArrayList<>();

  protected ElasticsearchDao(TransportClient client, ColumnMetadataDao columnMetadataDao, ElasticsearchSearchSubmitter searchSubmitter, AccessConfig config) {
    this.client = client;
    this.columnMetadataDao = columnMetadataDao;
    this.searchSubmitter = searchSubmitter;
    this.accessConfig = config;
    this.ignoredIndices.add(".kibana");
  }

  public ElasticsearchDao() {
    //uninitialized.
  }

  @Override
  public SearchResponse search(SearchRequest searchRequest) throws InvalidSearchException {
    return search(searchRequest, new QueryStringQueryBuilder(searchRequest.getQuery()));
  }

  /**
   * Defers to a provided {@link org.elasticsearch.index.query.QueryBuilder} for the query.
   * @param request The request defining the parameters of the search
   * @param queryBuilder The actual query to be run. Intended for if the SearchRequest requires wrapping
   * @return The results of the query
   * @throws InvalidSearchException When the query is malformed or the current state doesn't allow search
   */
  protected SearchResponse search(SearchRequest request, QueryBuilder queryBuilder) throws InvalidSearchException {
    org.elasticsearch.action.search.SearchRequest esRequest;
    org.elasticsearch.action.search.SearchResponse esResponse;

    if(client == null) {
      throw new InvalidSearchException("Uninitialized Dao!  You must call init() prior to use.");
    }

    if (request.getSize() > accessConfig.getMaxSearchResults()) {
      throw new InvalidSearchException("Search result size must be less than " + accessConfig.getMaxSearchResults());
    }

    esRequest = buildSearchRequest(request, queryBuilder);
    esResponse = searchSubmitter.submitSearch(esRequest);
    return buildSearchResponse(request, esResponse);
  }

  /**
   * Builds an Elasticsearch search request.
   * @param searchRequest The Metron search request.
   * @param queryBuilder
   * @return An Elasticsearch search request.
   */
  private org.elasticsearch.action.search.SearchRequest buildSearchRequest(
          SearchRequest searchRequest,
          QueryBuilder queryBuilder) throws InvalidSearchException {

    LOG.debug("Got search request; request={}", ElasticsearchUtils.toJSON(searchRequest));
    SearchSourceBuilder searchBuilder = new SearchSourceBuilder()
            .size(searchRequest.getSize())
            .from(searchRequest.getFrom())
            .query(queryBuilder)
            .trackScores(true);

    // column metadata needed to understand the type of each sort field
    Map<String, Map<String, FieldType>> meta;
    try {
      meta = getColumnMetadata(searchRequest.getIndices());
    } catch(IOException e) {
      throw new InvalidSearchException("Unable to get column metadata", e);
    }

    // handle sort fields
    for(SortField sortField : searchRequest.getSort()) {

      // what type is the sort field?
      FieldType sortFieldType = meta
              .values()
              .stream()
              .filter(e -> e.containsKey(sortField.getField()))
              .map(m -> m.get(sortField.getField()))
              .findFirst()
              .orElse(FieldType.OTHER);

      // sort order - if ASC, then missing values sorted last.  Otherwise, missing values sorted first
      org.elasticsearch.search.sort.SortOrder sortOrder = getElasticsearchSortOrder(sortField.getSortOrder());
      String missingSortOrder;
      if(sortOrder == org.elasticsearch.search.sort.SortOrder.DESC) {
        missingSortOrder = MISSING_SORT_LAST;
      } else {
        missingSortOrder = MISSING_SORT_FIRST;
      }

      // sort by the field - missing fields always last
      FieldSortBuilder sortBy = new FieldSortBuilder(sortField.getField())
              .order(sortOrder)
              .missing(missingSortOrder)
              .unmappedType(sortFieldType.getFieldType());
      searchBuilder.sort(sortBy);
    }

    // handle search fields
    if (searchRequest.getFields().isPresent()) {
      searchBuilder.fields(searchRequest.getFields().get());
    } else {
      searchBuilder.fetchSource(true);
    }

    // handle facet fields
    if (searchRequest.getFacetFields().isPresent()) {
      for(String field : searchRequest.getFacetFields().get()) {
        String name = getFacentAggregationName(field);
        TermsBuilder terms = new TermsBuilder(name).field(field);
        searchBuilder.aggregation(terms);
      }
    }

    // return the search request
    String[] indices = wildcardIndices(searchRequest.getIndices());
    LOG.debug("Built Elasticsearch request; indices={}, request={}", indices, searchBuilder.toString());
    return new org.elasticsearch.action.search.SearchRequest()
            .indices(indices)
            .source(searchBuilder);
  }

  /**
   * Builds a search response.
   *
   * This effectively transforms an Elasticsearch search response into a Metron search response.
   *
   * @param searchRequest The Metron search request.
   * @param esResponse The Elasticsearch search response.
   * @return A Metron search response.
   * @throws InvalidSearchException
   */
  private SearchResponse buildSearchResponse(
          SearchRequest searchRequest,
          org.elasticsearch.action.search.SearchResponse esResponse) throws InvalidSearchException {

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setTotal(esResponse.getHits().getTotalHits());

    // search hits --> search results
    List<SearchResult> results = new ArrayList<>();
    for(SearchHit hit: esResponse.getHits().getHits()) {
      results.add(getSearchResult(hit, searchRequest.getFields().isPresent()));
    }
    searchResponse.setResults(results);

    // handle facet fields
    if (searchRequest.getFacetFields().isPresent()) {
      List<String> facetFields = searchRequest.getFacetFields().get();
      Map<String, FieldType> commonColumnMetadata;
      try {
        commonColumnMetadata = getCommonColumnMetadata(searchRequest.getIndices());

      } catch (IOException e) {
        throw new InvalidSearchException(String.format(
                "Could not get common column metadata for indices %s",
                Arrays.toString(searchRequest.getIndices().toArray())));
      }
      searchResponse.setFacetCounts(getFacetCounts(facetFields, esResponse.getAggregations(), commonColumnMetadata ));
    }

    LOG.debug("Built search response; response={}", ElasticsearchUtils.toJSON(searchResponse));
    return searchResponse;
  }

  @Override
  public GroupResponse group(GroupRequest groupRequest) throws InvalidSearchException {
    return group(groupRequest, new QueryStringQueryBuilder(groupRequest.getQuery()));
  }

  /**
   * Defers to a provided {@link org.elasticsearch.index.query.QueryBuilder} for the query.
   * @param groupRequest The request defining the parameters of the grouping
   * @param queryBuilder The actual query to be run. Intended for if the SearchRequest requires wrapping
   * @return The results of the query
   * @throws InvalidSearchException When the query is malformed or the current state doesn't allow search
   */
  protected GroupResponse group(GroupRequest groupRequest, QueryBuilder queryBuilder)
      throws InvalidSearchException {
    org.elasticsearch.action.search.SearchRequest esRequest;
    org.elasticsearch.action.search.SearchResponse esResponse;

    if (client == null) {
      throw new InvalidSearchException("Uninitialized Dao!  You must call init() prior to use.");
    }
    if (groupRequest.getGroups() == null || groupRequest.getGroups().size() == 0) {
      throw new InvalidSearchException("At least 1 group must be provided.");
    }

    esRequest = buildGroupRequest(groupRequest, queryBuilder);
    esResponse = searchSubmitter.submitSearch(esRequest);
    GroupResponse response = buildGroupResponse(groupRequest, esResponse);

    return response;
  }

  /**
   * Builds a group search request.
   * @param groupRequest The Metron group request.
   * @param queryBuilder The search query.
   * @return An Elasticsearch search request.
   */
  private org.elasticsearch.action.search.SearchRequest buildGroupRequest(
          GroupRequest groupRequest,
          QueryBuilder queryBuilder) {

    // handle groups
    TermsBuilder groups = getGroupsTermBuilder(groupRequest, 0);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(queryBuilder)
            .aggregation(groups);

    // return the search request
    String[] indices = wildcardIndices(groupRequest.getIndices());
    return new org.elasticsearch.action.search.SearchRequest()
            .indices(indices)
            .source(searchSourceBuilder);
  }

  /**
   * Build a group response.
   * @param groupRequest The original group request.
   * @param response The search response.
   * @return A group response.
   * @throws InvalidSearchException
   */
  private GroupResponse buildGroupResponse(
          GroupRequest groupRequest,
          org.elasticsearch.action.search.SearchResponse response) throws InvalidSearchException {

    // build the search response
    Map<String, FieldType> commonColumnMetadata;
    try {
      commonColumnMetadata = getCommonColumnMetadata(groupRequest.getIndices());
    } catch (IOException e) {
      throw new InvalidSearchException(String.format("Could not get common column metadata for indices %s",
              Arrays.toString(groupRequest.getIndices().toArray())));
    }

    GroupResponse groupResponse = new GroupResponse();
    groupResponse.setGroupedBy(groupRequest.getGroups().get(0).getField());
    groupResponse.setGroupResults(getGroupResults(groupRequest, 0, response.getAggregations(), commonColumnMetadata));
    return groupResponse;
  }

  private String[] wildcardIndices(List<String> indices) {
    if(indices == null)
      return new String[] {};

    return indices
            .stream()
            .map(index -> String.format("%s%s*", index, INDEX_NAME_DELIMITER))
            .toArray(value -> new String[indices.size()]);
  }

  @Override
  public synchronized void init(AccessConfig config) {
    if(this.client == null) {
      this.client = ElasticsearchUtils.getClient(config.getGlobalConfigSupplier().get(), config.getOptionalSettings());
      this.accessConfig = config;
      this.columnMetadataDao = new ElasticsearchColumnMetadataDao(this.client.admin(), Collections.singletonList(".kibana"));
      this.searchSubmitter = new ElasticsearchSearchSubmitter(this.client);
    }

    if(columnMetadataDao == null) {
      throw new IllegalArgumentException("No ColumnMetadataDao available");
    }

    if(searchSubmitter == null) {
      throw new IllegalArgumentException("No ElasticsearchSearchSubmitter available");
    }
  }

  @Override
  public Document getLatest(final String guid, final String sensorType) throws IOException {
    Optional<Document> doc = searchByGuid(guid, sensorType, hit -> toDocument(guid, hit));
    return doc.orElse(null);
  }

  private Optional<Document> toDocument(final String guid, SearchHit hit) {
    Long ts = 0L;
    String doc = hit.getSourceAsString();
    String sourceType = toSourceType(hit.getType());
    try {
      return Optional.of(new Document(doc, guid, sourceType, ts));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to retrieve latest: " + e.getMessage(), e);
    }
  }

  /**
   * Returns the source type based on a given doc type.
   * @param docType The document type.
   * @return The source type.
   */
  private String toSourceType(String docType) {
    return Iterables.getFirst(Splitter.on("_doc").split(docType), null);
  }

  /**
   * Return the search hit based on the UUID and sensor type.
   * A callback can be specified to transform the hit into a type T.
   * If more than one hit happens, the first one will be returned.
   */
  <T> Optional<T> searchByGuid(String guid, String sensorType,
      Function<SearchHit, Optional<T>> callback) {
    QueryBuilder query;
    if (sensorType != null) {
      query = QueryBuilders.idsQuery(sensorType + "_doc").ids(guid);
    } else {
      query = QueryBuilders.idsQuery().ids(guid);
    }
    SearchRequestBuilder request = client.prepareSearch()
                                         .setQuery(query)
                                         .setSource("message")
                                         ;
    org.elasticsearch.action.search.SearchResponse response = request.get();
    SearchHits hits = response.getHits();
    long totalHits = hits.getTotalHits();
    if (totalHits > 1) {
      LOG.warn("Encountered {} results for guid {} in sensor {}. Returning first hit.",
          totalHits,
          guid,
          sensorType
      );
    }
    for (SearchHit hit : hits) {
      Optional<T> ret = callback.apply(hit);
      if (ret.isPresent()) {
        return ret;
      }
    }
    return Optional.empty();
  }

  @Override
  public void update(Document update, Optional<String> index) throws IOException {
    String indexPostfix = ElasticsearchUtils
        .getIndexFormat(accessConfig.getGlobalConfigSupplier().get()).format(new Date());
    String sensorType = update.getSensorType();
    String indexName = ElasticsearchUtils.getIndexName(sensorType, indexPostfix, null);
    String existingIndex = calculateExistingIndex(update, index, indexPostfix);

    UpdateRequest updateRequest = buildUpdateRequest(update, sensorType, indexName, existingIndex);

    org.elasticsearch.action.search.SearchResponse result = client.prepareSearch("test*")
        .setFetchSource(true)
        .setQuery(QueryBuilders.matchAllQuery())
        .get();
    result.getHits();
    try {
      UpdateResponse response = client.update(updateRequest).get();

      ShardInfo shardInfo = response.getShardInfo();
      int failed = shardInfo.getFailed();
      if (failed > 0) {
        throw new IOException(
            "ElasticsearchDao upsert failed: " + Arrays.toString(shardInfo.getFailures()));
      }
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void batchUpdate(Map<Document, Optional<String>> updates) throws IOException {
    String indexPostfix = ElasticsearchUtils
        .getIndexFormat(accessConfig.getGlobalConfigSupplier().get()).format(new Date());

    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

    // Get the indices we'll actually be using for each Document.
    for (Map.Entry<Document, Optional<String>> updateEntry : updates.entrySet()) {
      Document update = updateEntry.getKey();
      String sensorType = update.getSensorType();
      String indexName = ElasticsearchUtils.getIndexName(sensorType, indexPostfix, null);
      String existingIndex = calculateExistingIndex(update, updateEntry.getValue(), indexPostfix);
      UpdateRequest updateRequest = buildUpdateRequest(
          update,
          sensorType,
          indexName,
          existingIndex
      );

      bulkRequestBuilder.add(updateRequest);
    }

    BulkResponse bulkResponse = bulkRequestBuilder.get();
    if (bulkResponse.hasFailures()) {
      LOG.error("Bulk Request has failures: {}", bulkResponse.buildFailureMessage());
      throw new IOException(
          "ElasticsearchDao upsert failed: " + bulkResponse.buildFailureMessage());
    }
  }

  protected String calculateExistingIndex(Document update, Optional<String> index,
      String indexPostFix) {
    String sensorType = update.getSensorType();
    String indexName = ElasticsearchUtils.getIndexName(sensorType, indexPostFix, null);

    return index.orElse(
        searchByGuid(update.getGuid(),
            sensorType,
            hit -> Optional.ofNullable(hit.getIndex())
        ).orElse(indexName)
    );
  }

  protected UpdateRequest buildUpdateRequest(Document update, String sensorType, String indexName,
      String existingIndex) {
    String type = sensorType + "_doc";
    Object ts = update.getTimestamp();
    IndexRequest indexRequest = new IndexRequest(indexName, type, update.getGuid())
        .source(update.getDocument());
    if(ts != null) {
      indexRequest = indexRequest.timestamp(ts.toString());
    }

    return new UpdateRequest(existingIndex, type, update.getGuid())
        .doc(update.getDocument())
        .upsert(indexRequest);
  }

  @Override
  public Map<String, Map<String, FieldType>> getColumnMetadata(List<String> indices) throws IOException {
    return columnMetadataDao.getColumnMetadata(indices);
  }

  @Override
  public Map<String, FieldType> getCommonColumnMetadata(List<String> indices) throws IOException {
    return columnMetadataDao.getCommonColumnMetadata(indices);
  }

  private org.elasticsearch.search.sort.SortOrder getElasticsearchSortOrder(
      org.apache.metron.indexing.dao.search.SortOrder sortOrder) {
    return sortOrder == org.apache.metron.indexing.dao.search.SortOrder.DESC ?
        org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC;
  }

  private Order getElasticsearchGroupOrder(GroupOrder groupOrder) {
    if (groupOrder.getGroupOrderType() == GroupOrderType.TERM) {
      return groupOrder.getSortOrder() == SortOrder.ASC ? Order.term(true) : Order.term(false);
    } else {
      return groupOrder.getSortOrder() == SortOrder.ASC ? Order.count(true) : Order.count(false);
    }
  }

  public Map<String, Map<String, Long>> getFacetCounts(List<String> fields, Aggregations aggregations, Map<String, FieldType> commonColumnMetadata) {
    Map<String, Map<String, Long>> fieldCounts = new HashMap<>();
    for (String field: fields) {
      Map<String, Long> valueCounts = new HashMap<>();
      Aggregation aggregation = aggregations.get(getFacentAggregationName(field));
      if (aggregation instanceof Terms) {
        Terms terms = (Terms) aggregation;
        terms.getBuckets().stream().forEach(bucket -> valueCounts.put(formatKey(bucket.getKey(), commonColumnMetadata.get(field)), bucket.getDocCount()));
      }
      fieldCounts.put(field, valueCounts);
    }
    return fieldCounts;
  }

  private String formatKey(Object key, FieldType type) {
    if (FieldType.IP.equals(type)) {
      return IpFieldMapper.longToIp((Long) key);
    } else if (FieldType.BOOLEAN.equals(type)) {
      return (Long) key == 1 ? "true" : "false";
    } else {
      return key.toString();
    }
  }

  private TermsBuilder getGroupsTermBuilder(GroupRequest groupRequest, int index) {
    List<Group> groups = groupRequest.getGroups();
    Group group = groups.get(index);
    String aggregationName = getGroupByAggregationName(group.getField());
    TermsBuilder termsBuilder = new TermsBuilder(aggregationName)
        .field(group.getField())
        .size(accessConfig.getMaxSearchGroups())
        .order(getElasticsearchGroupOrder(group.getOrder()));
    if (index < groups.size() - 1) {
      termsBuilder.subAggregation(getGroupsTermBuilder(groupRequest, index + 1));
    }
    Optional<String> scoreField = groupRequest.getScoreField();
    if (scoreField.isPresent()) {
      termsBuilder.subAggregation(new SumBuilder(getSumAggregationName(scoreField.get())).field(scoreField.get()).missing(0));
    }
    return termsBuilder;
  }

  private List<GroupResult> getGroupResults(GroupRequest groupRequest, int index, Aggregations aggregations, Map<String, FieldType> commonColumnMetadata) {
    List<Group> groups = groupRequest.getGroups();
    String field = groups.get(index).getField();
    Terms terms = aggregations.get(getGroupByAggregationName(field));
    List<GroupResult> searchResultGroups = new ArrayList<>();
    for(Bucket bucket: terms.getBuckets()) {
      GroupResult groupResult = new GroupResult();
      groupResult.setKey(formatKey(bucket.getKey(), commonColumnMetadata.get(field)));
      groupResult.setTotal(bucket.getDocCount());
      Optional<String> scoreField = groupRequest.getScoreField();
      if (scoreField.isPresent()) {
        Sum score = bucket.getAggregations().get(getSumAggregationName(scoreField.get()));
        groupResult.setScore(score.getValue());
      }
      if (index < groups.size() - 1) {
        groupResult.setGroupedBy(groups.get(index + 1).getField());
        groupResult.setGroupResults(getGroupResults(groupRequest, index + 1, bucket.getAggregations(), commonColumnMetadata));
      }
      searchResultGroups.add(groupResult);
    }
    return searchResultGroups;
  }

  private SearchResult getSearchResult(SearchHit searchHit, boolean fieldsPresent) {
    SearchResult searchResult = new SearchResult();
    searchResult.setId(searchHit.getId());
    Map<String, Object> source;
    if (fieldsPresent) {
      source = new HashMap<>();
      searchHit.getFields().forEach((key, value) -> {
        source.put(key, value.getValues().size() == 1 ? value.getValue() : value.getValues());
      });
    } else {
      source = searchHit.getSource();
    }
    searchResult.setSource(source);
    searchResult.setScore(searchHit.getScore());
    searchResult.setIndex(searchHit.getIndex());
    return searchResult;
  }

  private String getFacentAggregationName(String field) {
    return String.format("%s_count", field);
  }

  public TransportClient getClient() {
    return client;
  }

  private String getGroupByAggregationName(String field) {
    return String.format("%s_group", field);
  }

  private String getSumAggregationName(String field) {
    return String.format("%s_score", field);
  }

  public ElasticsearchDao client(TransportClient client) {
    this.client = client;
    return this;
  }

  public ElasticsearchDao columnMetadataDao(ColumnMetadataDao columnMetadataDao) {
    this.columnMetadataDao = columnMetadataDao;
    return this;
  }

  public ElasticsearchDao accessConfig(AccessConfig accessConfig) {
    this.accessConfig = accessConfig;
    return this;
  }

  public ElasticsearchDao ignoredIndices(List<String> ignoredIndices) {
    this.ignoredIndices = ignoredIndices;
    return this;
  }
}
