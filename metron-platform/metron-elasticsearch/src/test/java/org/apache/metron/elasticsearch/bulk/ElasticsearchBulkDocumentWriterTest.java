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
package org.apache.metron.elasticsearch.bulk;

import org.apache.metron.common.Constants;
import org.apache.metron.elasticsearch.client.ElasticsearchClient;
import org.apache.metron.indexing.dao.update.Document;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchBulkDocumentWriterTest {

    ElasticsearchBulkDocumentWriter<Document> writer;
    ElasticsearchClient client;
    RestHighLevelClient highLevelClient;
    boolean onSuccessCalled;
    boolean onFailureCalled;

    @Before
    public void setup() {
        // mock Elasticsearch
        highLevelClient = mock(RestHighLevelClient.class);
        client = mock(ElasticsearchClient.class);
        when(client.getHighLevelClient()).thenReturn(highLevelClient);

        writer = new ElasticsearchBulkDocumentWriter<>(client);
        onFailureCalled = false;
        onSuccessCalled = false;
    }

    @Test
    public void testSuccessCallback() throws IOException {
        setupElasticsearchToSucceed();

        // create a document to write
        Document doc = document(message());
        String index = "bro_index";
        writer.addDocument(doc, index);

        // validate the "on success" callback
        writer.onSuccess(successfulDocs -> {
            assertEquals(1, successfulDocs.size());
            assertEquals(doc, successfulDocs.get(0));
            onSuccessCalled = true;
        });

        writer.write();
        assertTrue(onSuccessCalled);
        assertFalse(onFailureCalled);
    }

    @Test
    public void testSuccessWithNoCallbacks() throws IOException {
        setupElasticsearchToSucceed();

        // create a document to write
        Document doc = document(message());
        String index = "bro_index";
        writer.addDocument(doc, index);

        // no callbacks defined
        writer.write();
        assertFalse(onSuccessCalled);
        assertFalse(onFailureCalled);
    }

    @Test
    public void testFailureCallback() throws IOException {
        setupElasticsearchToFail();

        // create a document to write
        Document doc = document(message());
        String index = "bro_index";
        writer.addDocument(doc, index);

        // validate the "on failure" callback
        writer.onFailure((failedDoc, cause, msg) -> {
            assertEquals(doc, failedDoc);
            onFailureCalled = true;
        });

        // no callbacks defined
        writer.write();
        assertFalse(onSuccessCalled);
        assertTrue(onFailureCalled);
    }

    @Test
    public void testFailureWithNoCallbacks() throws IOException {
        setupElasticsearchToFail();

        // create a document to write
        Document doc = document(message());
        String index = "bro_index";
        writer.addDocument(doc, index);

        // no callbacks defined
        writer.write();
        assertFalse(onSuccessCalled);
        assertFalse(onFailureCalled);
    }

    @Test
    public void testFlushBatchOnSuccess() throws IOException {
        setupElasticsearchToSucceed();
        assertEquals(0, writer.size());

        // add some documents to write
        String index = "bro_index";
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        assertEquals(5, writer.size());

        // after the write, all documents should have been flushed
        writer.write();
        assertEquals(0, writer.size());
    }

    @Test
    public void testFlushBatchOnFailure() throws IOException {
        setupElasticsearchToFail();
        assertEquals(0, writer.size());

        // add some documents to write
        String index = "bro_index";
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        writer.addDocument(document(message()), index);
        assertEquals(5, writer.size());

        // after the write, all documents should have been flushed
        writer.write();
        assertEquals(0, writer.size());
    }

    private void setupElasticsearchToFail() throws IOException {
        // define the item failure
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        when(failure.getCause()).thenReturn(new Exception("test exception"));
        when(failure.getMessage()).thenReturn("error message");

        // define the item level response
        BulkItemResponse itemResponse = mock(BulkItemResponse.class);
        when(itemResponse.isFailed()).thenReturn(true);
        when(itemResponse.getItemId()).thenReturn(0);
        when(itemResponse.getFailure()).thenReturn(failure);
        List<BulkItemResponse> itemsResponses = Collections.singletonList(itemResponse);

        // define the bulk response to indicate failure
        BulkResponse response = mock(BulkResponse.class);
        when(response.iterator()).thenReturn(itemsResponses.iterator());
        when(response.hasFailures()).thenReturn(true);

        // have the client return the mock response
        when(highLevelClient.bulk(any(BulkRequest.class))).thenReturn(response);
    }

    private void setupElasticsearchToSucceed() throws IOException {
        // define the bulk response to indicate success
        BulkResponse response = mock(BulkResponse.class);
        when(response.hasFailures()).thenReturn(false);

        // have the client return the mock response
        when(highLevelClient.bulk(any(BulkRequest.class))).thenReturn(response);
    }

    private Document document(JSONObject message) {
        String guid = UUID.randomUUID().toString();
        String sensorType = "bro";
        Long timestamp = System.currentTimeMillis();
        return new Document(message, guid, sensorType, timestamp);
    }

    private JSONObject message() {
        JSONObject message = new JSONObject();
        message.put(Constants.GUID, UUID.randomUUID().toString());
        message.put(Constants.Fields.TIMESTAMP.getName(), System.currentTimeMillis());
        message.put(Constants.Fields.SRC_ADDR.getName(), "192.168.1.1");
        return message;
    }
}
