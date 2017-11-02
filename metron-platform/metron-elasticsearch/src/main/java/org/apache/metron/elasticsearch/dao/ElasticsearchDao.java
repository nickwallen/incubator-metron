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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ActionWriteResponse.ShardInfo;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.RestStatus;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.metron.elasticsearch.utils.ElasticsearchUtils.INDEX_NAME_DELIMITER;

public class ElasticsearchDao implements IndexDao {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private transient TransportClient client;
  private AccessConfig accessConfig;
  private List<String> ignoredIndices = new ArrayList<>();

  protected ElasticsearchDao(TransportClient client, AccessConfig config) {
    this.client = client;
    this.accessConfig = config;
    this.ignoredIndices.add(".kibana");
  }

  public ElasticsearchDao() {
    //uninitialized.
  }

  private static Map<String, FieldType> elasticsearchSearchTypeMap;

  static {
    Map<String, FieldType> fieldTypeMap = new HashMap<>();
    fieldTypeMap.put("string", FieldType.STRING);
    fieldTypeMap.put("ip", FieldType.IP);
    fieldTypeMap.put("integer", FieldType.INTEGER);
    fieldTypeMap.put("long", FieldType.LONG);
    fieldTypeMap.put("date", FieldType.DATE);
    fieldTypeMap.put("float", FieldType.FLOAT);
    fieldTypeMap.put("double", FieldType.DOUBLE);
    fieldTypeMap.put("boolean", FieldType.BOOLEAN);
    elasticsearchSearchTypeMap = Collections.unmodifiableMap(fieldTypeMap);
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
    esResponse = executeSearch(esRequest);
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
          QueryBuilder queryBuilder)
  {
    LOG.debug("Got search request; request={}", toJSON(searchRequest));
    SearchSourceBuilder searchBuilder = new SearchSourceBuilder()
            .size(searchRequest.getSize())
            .from(searchRequest.getFrom())
            .query(queryBuilder)
            .trackScores(true);

    // handle sort fields
    for(SortField sortField : searchRequest.getSort()) {
      FieldSortBuilder sortBy = new FieldSortBuilder(sortField.getField())
              .order(getElasticsearchSortOrder(sortField.getSortOrder()));
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
   * Executes a search against Elasticsearch.
   * @param request The search request.
   * @return The search response.
   * @throws InvalidSearchException If the search execution fails for any reason.
   */
  private org.elasticsearch.action.search.SearchResponse executeSearch(
          org.elasticsearch.action.search.SearchRequest request) throws InvalidSearchException {

    // submit the search request
    org.elasticsearch.action.search.SearchResponse esResponse;
    try {
      esResponse = client
              .search(request)
              .actionGet();

    } catch (SearchPhaseExecutionException e) {
      String msg = String.format(
              "Failed to execute search; error='%s', search='%s'",
              ExceptionUtils.getRootCauseMessage(e),
              toJSON(request));
      LOG.error(msg, e);
      throw new InvalidSearchException(msg, e);
    }

    // check for shard failures
    LOG.debug("Got Elasticsearch response; response={}", esResponse.toString());
    if(esResponse.getFailedShards() > 0) {
      logShardFailures(request, esResponse);
    }

    // validate the response status
    if(RestStatus.OK == esResponse.status()) {
      return esResponse;

    } else {
      // the search was not successful
      String msg = String.format(
                "Bad search response; status=%s, timeout=%s, terminatedEarly=%s",
                esResponse.status(), esResponse.isTimedOut(), esResponse.isTerminatedEarly());
      LOG.error(msg);
      throw new InvalidSearchException(msg);
    }
  }

  /**
   * Log individual shard failures that can occur even when the response is OK.  These
   * can indicate misconfiguration and are important to log.
   *
   * @param request The search request.
   * @param response  The search response.
   */
  private void logShardFailures(
          org.elasticsearch.action.search.SearchRequest request,
          org.elasticsearch.action.search.SearchResponse response) {

    LOG.warn("Search resulted in {}/{} shards failing; errors={}, search={}",
            response.getFailedShards(),
            response.getTotalShards(),
            ArrayUtils.getLength(response.getShardFailures()),
            toJSON(request));

    // log each reported failure
    for(ShardSearchFailure fail: response.getShardFailures()) {
      String msg = String.format(
              "Shard search failure; reason=%s, index=%s, shard=%s, status=%s, nodeId=%s",
              ExceptionUtils.getRootCauseMessage(fail.getCause()),
              fail.index(),
              fail.shardId(),
              fail.status(),
              fail.shard().getNodeId());
      LOG.warn(msg, fail.getCause());
    }
  }

  /**
   * Transforms an Elasticsearch search response into a Metron search response.
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
    final boolean fieldsPresent = searchRequest.getFields().isPresent();
    List<SearchResult> results = new ArrayList<>();
    for(SearchHit hit: esResponse.getHits().getHits()) {
      SearchResult result = getSearchResult(hit, fieldsPresent);
      results.add(result);
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

    LOG.debug("Built search response; response={}", toJSON(searchResponse));
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
    org.elasticsearch.action.search.SearchRequest request;
    org.elasticsearch.action.search.SearchResponse response;

    if (client == null) {
      throw new InvalidSearchException("Uninitialized Dao!  You must call init() prior to use.");
    }
    if (groupRequest.getGroups() == null || groupRequest.getGroups().size() == 0) {
      throw new InvalidSearchException("At least 1 group must be provided.");
    }

    // build the search request
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(queryBuilder)
            .aggregation(getGroupsTermBuilder(groupRequest, 0));
    String[] indices = wildcardIndices(groupRequest.getIndices());
    request = new org.elasticsearch.action.search.SearchRequest(indices)
            .source(searchSourceBuilder);

    // search
    response = executeSearch(request);

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
        .source(update.getDocument())
        ;
    if(ts != null) {
      indexRequest = indexRequest.timestamp(ts.toString());
    }

    return new UpdateRequest(existingIndex, type, update.getGuid())
        .doc(update.getDocument())
        .upsert(indexRequest);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Map<String, FieldType>> getColumnMetadata(List<String> indices) throws IOException {
    Map<String, Map<String, FieldType>> allColumnMetadata = new HashMap<>();
    String[] latestIndices = getLatestIndices(indices);
    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client
            .admin()
            .indices()
            .getMappings(new GetMappingsRequest().indices(latestIndices))
            .actionGet()
            .getMappings();
    for(Object key: mappings.keys().toArray()) {
      String indexName = key.toString();

      Map<String, FieldType> indexColumnMetadata = new HashMap<>();
      ImmutableOpenMap<String, MappingMetaData> mapping = mappings.get(indexName);
      Iterator<String> mappingIterator = mapping.keysIt();
      while(mappingIterator.hasNext()) {
        MappingMetaData mappingMetaData = mapping.get(mappingIterator.next());
        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) mappingMetaData.getSourceAsMap().get("properties");
        for(String field: map.keySet()) {
          indexColumnMetadata.put(field, elasticsearchSearchTypeMap.getOrDefault(map.get(field).get("type"), FieldType.OTHER));
        }
      }

      String baseIndexName = ElasticsearchUtils.getBaseIndexName(indexName);
      allColumnMetadata.put(baseIndexName, indexColumnMetadata);
    }
    return allColumnMetadata;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, FieldType> getCommonColumnMetadata(List<String> indices) throws IOException {
    LOG.debug("Getting common metadata; indices={}", indices);
    Map<String, FieldType> commonColumnMetadata = null;

    // retrieve the mappings for only the latest version of each index
    String[] latestIndices = getLatestIndices(indices);
    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings;
    mappings = client
            .admin()
            .indices()
            .getMappings(new GetMappingsRequest().indices(latestIndices))
            .actionGet()
            .getMappings();

    // did we get all the mappings that we expect?
    if(mappings.size() < latestIndices.length) {
      String msg = String.format(
              "Failed to get required mappings; expected mappings for '%s', but got '%s'",
              latestIndices, mappings.keys().toArray());
      throw new IllegalStateException(msg);
    }

    // for each index...
    for(Object index: mappings.keys().toArray()) {
      ImmutableOpenMap<String, MappingMetaData> mapping = mappings.get(index.toString());
      Iterator<String> mappingIterator = mapping.keysIt();
      while(mappingIterator.hasNext()) {
        MappingMetaData metadata = mapping.get(mappingIterator.next());
        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) metadata.getSourceAsMap().get("properties");
        Map<String, FieldType> mappingsWithTypes = map
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e-> elasticsearchSearchTypeMap.getOrDefault(e.getValue().get("type"), FieldType.OTHER)));

        // keep only the properties in common
        if (commonColumnMetadata == null) {
          commonColumnMetadata = mappingsWithTypes;
        } else {
          commonColumnMetadata
                  .entrySet()
                  .retainAll(mappingsWithTypes.entrySet());
        }
      }
    }

    return commonColumnMetadata;
  }

  protected String[] getLatestIndices(List<String> includeIndices) {
    LOG.debug("Getting latest indices; indices={}", includeIndices);
    Map<String, String> latestIndices = new HashMap<>();
    String[] indices = client
            .admin()
            .indices()
            .prepareGetIndex()
            .setFeatures()
            .get()
            .getIndices();
    for (String index : indices) {
      if (!ignoredIndices.contains(index)) {
        int prefixEnd = index.indexOf(INDEX_NAME_DELIMITER);
        if (prefixEnd != -1) {
          String prefix = index.substring(0, prefixEnd);
          if (includeIndices.contains(prefix)) {
            String latestIndex = latestIndices.get(prefix);
            if (latestIndex == null || index.compareTo(latestIndex) > 0) {
              latestIndices.put(prefix, index);
            }
          }
        }
      }
    }
    return latestIndices.values().toArray(new String[latestIndices.size()]);
  }

  /**
   * Converts an Elasticsearch SearchRequest to JSON.
   * @param esRequest The search request.
   * @return The JSON representation of the SearchRequest.
   */
  public static String toJSON(org.elasticsearch.action.search.SearchRequest esRequest) {
    String json = "null";
    if(esRequest != null) {
      try {
        json = XContentHelper.convertToJson(esRequest.source(), true);

      } catch (IOException io) {
        json = "JSON conversion failed; request=" + esRequest.toString();
      }
    }
    return json;
  }

  /**
   * Convert a SearchRequest to JSON.
   * @param request The search request.
   * @return The JSON representation of the SearchRequest.
   */
  public static String toJSON(Object request) {
    String json = "null";
    if(request != null) {
      try {
        json = new ObjectMapper()
                .writer()
                .withDefaultPrettyPrinter()
                .writeValueAsString(request);

      } catch (IOException io) {
        json = "JSON conversion failed; request=" + request.toString();
      }
    }

    return json;
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


}
