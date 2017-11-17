/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.metron.common.Constants;
import org.apache.metron.common.Constants.Fields;
import org.apache.metron.indexing.dao.AccessConfig;
import org.apache.metron.indexing.dao.IndexDao;
import org.apache.metron.indexing.dao.MetaAlertDao;
import org.apache.metron.indexing.dao.metaalert.MetaAlertCreateRequest;
import org.apache.metron.indexing.dao.metaalert.MetaAlertStatus;
import org.apache.metron.indexing.dao.search.FieldType;
import org.apache.metron.indexing.dao.search.GetRequest;
import org.apache.metron.indexing.dao.search.GroupRequest;
import org.apache.metron.indexing.dao.search.GroupResponse;
import org.apache.metron.indexing.dao.search.InvalidCreateException;
import org.apache.metron.indexing.dao.search.InvalidSearchException;
import org.apache.metron.indexing.dao.search.SearchRequest;
import org.apache.metron.indexing.dao.search.SearchResponse;
import org.apache.metron.indexing.dao.update.Document;
import org.junit.Test;

public class ElasticsearchMetaAlertDaoTest {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInit() {
    IndexDao dao = new IndexDao() {
      @Override
      public SearchResponse search(SearchRequest searchRequest) throws InvalidSearchException {
        return null;
      }

      @Override
      public GroupResponse group(GroupRequest groupRequest) throws InvalidSearchException {
        return null;
      }

      @Override
      public void init(AccessConfig config) {
      }

      @Override
      public Document getLatest(String guid, String sensorType) throws IOException {
        return null;
      }

      @Override
      public Iterable<Document> getAllLatest(
          List<GetRequest> getRequests) throws IOException {
        return null;
      }

      @Override
      public void update(Document update, Optional<String> index) throws IOException {
      }

      @Override
      public void batchUpdate(Map<Document, Optional<String>> updates) throws IOException {
      }

      @Override
      public Map<String, FieldType> getColumnMetadata(List<String> indices)
          throws IOException {
        return null;
      }
    };
    ElasticsearchMetaAlertDao metaAlertDao = new ElasticsearchMetaAlertDao();
    metaAlertDao.init(dao);
  }

  @Test
  public void testBuildCreateDocumentSingleAlert() throws InvalidCreateException, IOException {
    ElasticsearchDao esDao = new ElasticsearchDao();
    ElasticsearchMetaAlertDao emaDao = new ElasticsearchMetaAlertDao();
    emaDao.init(esDao);

    List<String> groups = new ArrayList<>();
    groups.add("group_one");
    groups.add("group_two");

    // Build the first response from the multiget
    Map<String, Object> alertOne = new HashMap<>();
    alertOne.put(Constants.GUID, "alert_one");
    alertOne.put(MetaAlertDao.THREAT_FIELD_DEFAULT, 10.0d);
    List<Document> alerts = new ArrayList<Document>() {{
      add(new Document(alertOne, "", "", 0L));
    }};

    // Actually build the doc
    Document actual = emaDao.buildCreateDocument(alerts, groups);

    ArrayList<Map<String, Object>> alertList = new ArrayList<>();
    alertList.add(alertOne);

    Map<String, Object> actualDocument = actual.getDocument();
    assertEquals(
        MetaAlertStatus.ACTIVE.getStatusString(),
        actualDocument.get(MetaAlertDao.STATUS_FIELD)
    );
    assertEquals(
        alertList,
        actualDocument.get(MetaAlertDao.ALERT_FIELD)
    );
    assertEquals(
        groups,
        actualDocument.get(MetaAlertDao.GROUPS_FIELD)
    );

    // Don't care about the result, just that it's a UUID. Exception will be thrown if not.
    UUID.fromString((String) actualDocument.get(Constants.GUID));
  }

  @Test
  public void testBuildCreateDocumentMultipleAlerts() throws InvalidCreateException, IOException {
    ElasticsearchDao esDao = new ElasticsearchDao();
    ElasticsearchMetaAlertDao emaDao = new ElasticsearchMetaAlertDao();
    emaDao.init(esDao);

    List<String> groups = new ArrayList<>();
    groups.add("group_one");
    groups.add("group_two");

    // Build the first response from the multiget
    Map<String, Object> alertOne = new HashMap<>();
    alertOne.put(Constants.GUID, "alert_one");
    alertOne.put(MetaAlertDao.THREAT_FIELD_DEFAULT, 10.0d);

    // Build the second response from the multiget
    Map<String, Object> alertTwo = new HashMap<>();
    alertTwo.put(Constants.GUID, "alert_one");
    alertTwo.put(MetaAlertDao.THREAT_FIELD_DEFAULT, 5.0d);
    List<Document> alerts = new ArrayList<Document>() {{
      add(new Document(alertOne, "", "", 0L));
      add(new Document(alertTwo, "", "", 0L));
    }};

    // Actually build the doc
    Document actual = emaDao.buildCreateDocument(alerts, groups);

    ArrayList<Map<String, Object>> alertList = new ArrayList<>();
    alertList.add(alertOne);
    alertList.add(alertTwo);

    Map<String, Object> actualDocument = actual.getDocument();
    assertNotNull(actualDocument.get(Fields.TIMESTAMP.getName()));
    assertEquals(
        alertList,
        actualDocument.get(MetaAlertDao.ALERT_FIELD)
    );
    assertEquals(
        groups,
        actualDocument.get(MetaAlertDao.GROUPS_FIELD)
    );

    // Don't care about the result, just that it's a UUID. Exception will be thrown if not.
    UUID.fromString((String) actualDocument.get(Constants.GUID));
  }

  @Test(expected = InvalidCreateException.class)
  public void testCreateMetaAlertEmptyGuids() throws InvalidCreateException, IOException {
    ElasticsearchDao esDao = new ElasticsearchDao();
    ElasticsearchMetaAlertDao emaDao = new ElasticsearchMetaAlertDao();
    emaDao.init(esDao);

    MetaAlertCreateRequest createRequest = new MetaAlertCreateRequest();
    emaDao.createMetaAlert(createRequest);
  }

  @Test(expected = InvalidCreateException.class)
  public void testCreateMetaAlertEmptyGroups() throws InvalidCreateException, IOException {
    ElasticsearchDao esDao = new ElasticsearchDao();
    ElasticsearchMetaAlertDao emaDao = new ElasticsearchMetaAlertDao();
    emaDao.init(esDao);

    MetaAlertCreateRequest createRequest = new MetaAlertCreateRequest();
    createRequest.setAlerts(Collections.singletonList(new GetRequest("don't", "care")));
    emaDao.createMetaAlert(createRequest);
  }

  @Test
  public void testCalculateMetaScoresList() {
    List<Map<String, Object>> alertList = new ArrayList<>();
    Map<String, Object> alertMap = new HashMap<>();
    alertMap.put(MetaAlertDao.THREAT_FIELD_DEFAULT, 10.0d);
    alertList.add(alertMap);
    Map<String, Object> docMap = new HashMap<>();
    docMap.put(MetaAlertDao.ALERT_FIELD, alertList);

    Document doc = new Document(docMap, "guid", MetaAlertDao.METAALERT_TYPE, 0L);

    ElasticsearchMetaAlertDao metaAlertDao = new ElasticsearchMetaAlertDao();
    metaAlertDao.calculateMetaScores(doc);
    assertEquals(1L, doc.getDocument().get("count"));
    assertEquals(10.0d,
        doc.getDocument().get(ElasticsearchMetaAlertDao.THREAT_FIELD_DEFAULT)
    );
  }
}
