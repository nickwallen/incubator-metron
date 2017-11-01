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

import org.apache.metron.elasticsearch.matcher.SearchRequestMatcher;
import org.apache.metron.indexing.dao.AccessConfig;
import org.apache.metron.indexing.dao.search.InvalidSearchException;
import org.apache.metron.indexing.dao.search.SearchRequest;
import org.apache.metron.indexing.dao.search.SearchResponse;
import org.apache.metron.indexing.dao.search.SearchResult;
import org.apache.metron.indexing.dao.search.SortField;
import org.apache.metron.indexing.dao.search.SortOrder;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticsearchDaoTest {

  @Test
  public void searchShouldProperlyBuildSearchRequest() throws Exception {

    // setup the mocks
    SearchHit hit1 = mock(SearchHit.class);
    when(hit1.getId()).thenReturn("id1");
    when(hit1.getSource()).thenReturn(new HashMap<String, Object>(){{ put("field", "value1"); }});
    when(hit1.getScore()).thenReturn(0.1f);

    SearchHit hit2 = mock(SearchHit.class);
    when(hit2.getId()).thenReturn("id2");
    when(hit2.getSource()).thenReturn(new HashMap<String, Object>(){{ put("field", "value2"); }});
    when(hit2.getScore()).thenReturn(0.2f);

    SearchHit[] hits = { hit1, hit2 };
    ElasticsearchDao dao = setup(hits, RestStatus.OK, 25);

    // "sort by" fields for the search request
    SortField[] sortFields = {
            sortBy("sortField1", SortOrder.DESC),
            sortBy("sortField2", SortOrder.ASC)
    };

    // create a search request
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setSize(2);
    searchRequest.setIndices(Arrays.asList("bro", "snort"));
    searchRequest.setFrom(5);
    searchRequest.setSort(Arrays.asList(sortFields));
    searchRequest.setQuery("some query");

    // submit the search request
    SearchResponse searchResponse = dao.search(searchRequest);

    // validate
    String[] expectedIndices = {"bro_index*", "snort_index*"};
    verify(dao.getClient()).search(argThat(new SearchRequestMatcher(expectedIndices, "some query", 2, 5, sortFields)));
    assertEquals(2, searchResponse.getTotal());

    // validate search hits
    List<SearchResult> actualSearchResults = searchResponse.getResults();
    assertEquals(2, actualSearchResults.size());

    // validate hit1
    assertEquals("id1", actualSearchResults.get(0).getId());
    assertEquals("value1", actualSearchResults.get(0).getSource().get("field"));
    assertEquals(0.1f, actualSearchResults.get(0).getScore(), 0.0f);

    // validate hit2
    assertEquals("id2", actualSearchResults.get(1).getId());
    assertEquals("value2", actualSearchResults.get(1).getSource().get("field"));
    assertEquals(0.2f, actualSearchResults.get(1).getScore(), 0.0f);
    verifyNoMoreInteractions(dao.getClient());
  }

  @Test(expected = InvalidSearchException.class)
  public void searchShouldThrowExceptionWhenMaxResultsAreExceeded() throws Exception {

    int maxSearchResults = 20;
    SearchHit[] hits = { };
    ElasticsearchDao dao = setup(hits, RestStatus.OK, maxSearchResults);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setSize(maxSearchResults+1);

    dao.search(searchRequest);
    // exception expected - size > max
  }

  @Test(expected = InvalidSearchException.class)
  public void searchShouldThrowExceptionWhenStatusNotOK() throws Exception {

    int maxSearchResults = 20;
    SearchHit[] hits = { };
    ElasticsearchDao dao = setup(hits, RestStatus.PARTIAL_CONTENT, maxSearchResults);

    dao.search(new SearchRequest());
    // exception expected - search result not OK
  }

  private ElasticsearchDao setup(SearchHit[] hits, RestStatus status, int maxSearchResults) {

    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits()).thenReturn(Integer.toUnsignedLong(hits.length));

    org.elasticsearch.action.search.SearchResponse response = mock(org.elasticsearch.action.search.SearchResponse.class);
    when(response.status()).thenReturn(status);
    when(response.getHits()).thenReturn(searchHits);

    ActionFuture future = mock(ActionFuture.class);
    when(future.actionGet()).thenReturn(response);

    TransportClient client = mock(TransportClient.class);
    when(client.search(any(org.elasticsearch.action.search.SearchRequest.class))).thenReturn(future);

    AccessConfig config = mock(AccessConfig.class);
    when(config.getMaxSearchResults()).thenReturn(maxSearchResults);

    return new ElasticsearchDao(client, config);
  }

  private SortField sortBy(String field, SortOrder order) {
    SortField sortField = new SortField();
    sortField.setField(field);
    sortField.setSortOrder(order.toString());
    return sortField;
  }

}
