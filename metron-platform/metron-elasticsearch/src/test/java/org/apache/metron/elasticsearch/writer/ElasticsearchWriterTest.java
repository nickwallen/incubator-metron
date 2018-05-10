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

package org.apache.metron.elasticsearch.writer;

import com.google.common.collect.ImmutableList;
import org.apache.metron.common.configuration.IndexingConfigurations;
import org.apache.metron.common.configuration.writer.IndexingWriterConfiguration;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.elasticsearch.integration.components.ElasticSearchComponent;
import org.apache.metron.integration.ComponentRunner;
import org.apache.metron.integration.InMemoryComponent;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchWriterTest {

//  private ComponentRunner runner;
//
//  @Before
//  public void setup() throws Exception {
//    runner = new ComponentRunner.Builder()
//            .withComponent("search", getSearchComponent())
//            .build();
//    runner.start();
//  }
//
//  @After
//  public void tearDown() throws Exception {
//    if(runner != null) {
//      runner.stop();
//    }
//  }
//
//  public InMemoryComponent getSearchComponent() {
//    return new ElasticSearchComponent.Builder()
//        .withHttpPort(9211)
//        .withIndexDir(new File("target/elasticsearch"))
//        //.withMapping(index, "yaf_doc", mapping)
//        .build();
//  }

  @Test
  public void testSingleSuccesses() throws Exception {
    Tuple tuple1 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(false);

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addSuccess(tuple1);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual = esWriter.buildWriterResponse(ImmutableList.of(tuple1), response);

    assertEquals("Response should have no errors and single success", expected, actual);
  }

//  @Test
//  public void testMultipleSuccessesNew() throws Exception {
//
//    JSONObject message1 = new JSONObject();
//    JSONObject message2 = new JSONObject();
//
//    Tuple tuple1 = mock(Tuple.class);
//    Tuple tuple2 = mock(Tuple.class);
//
//    // the response has 2 successes
//    BulkResponse response = mock(BulkResponse.class);
//    when(response.hasFailures()).thenReturn(false);
//    when(response.getItems()).thenReturn(new BulkItemResponse[2]);
//    when(response.hasFailures()).thenReturn(false);
//    when(response.getTook()).thenReturn(TimeValue.timeValueMillis(22));
//
//    BulkWriterResponse expected = new BulkWriterResponse();
//    expected.addSuccess(tuple1);
//    expected.addSuccess(tuple2);
//
//    Map<String, Object> globals = new HashMap<>();
//    globals.put("es.clustername", "metron");
//    globals.put("es.date.format", "yyyy.MM.dd.HH");
//    globals.put("es.port", "9300");
//    globals.put("es.ip", "localhost");
//
//    IndexingConfigurations configsForSensor = new IndexingConfigurations();
//    configsForSensor.updateGlobalConfig(globals);
//
//    configsForSensor.updateSensorIndexingConfig("sensor", Collections.emptyMap());
//    IndexingWriterConfiguration writerConfigurations =
//        new IndexingWriterConfiguration("elasticsearch", configsForSensor);
//
//    Map<String, Object> stormConf = new HashMap<>();
//    TopologyContext context = mock(TopologyContext.class);
//
//    ElasticsearchWriter esWriter = new ElasticsearchWriter();
//    esWriter.init(stormConf, context, writerConfigurations);
//
//    List<Tuple> tuples = new ArrayList<>();
//    tuples.add(tuple1);
//    tuples.add(tuple2);
//
//    List<JSONObject> messages = new ArrayList<>();
//    messages.add(message1);
//    messages.add(message2);
//
//    // TODO this is what we should be doing.
//    BulkWriterResponse actual = esWriter.write("sensor", writerConfigurations, tuples, messages);
//    // BulkWriterResponse actual = esWriter.buildWriteResponse(ImmutableList.of(tuple1, tuple2),
//    // response);
//
//    assertEquals("Response should have no errors and two successes", expected, actual);
//  }

  @Test
  public void testMultipleSuccesses() throws Exception {
    Tuple tuple1 = mock(Tuple.class);
    Tuple tuple2 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(false);

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addSuccess(tuple1);
    expected.addSuccess(tuple2);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual =
        esWriter.buildWriterResponse(ImmutableList.of(tuple1, tuple2), response);

    assertEquals("Response should have no errors and two successes", expected, actual);
  }

  @Test
  public void testSingleFailure() throws Exception {
    Tuple tuple1 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);

    Exception e = new IllegalStateException();
    BulkItemResponse itemResponse = buildBulkItemFailure(e);
    when(response.iterator()).thenReturn(ImmutableList.of(itemResponse).iterator());

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addError(e, tuple1);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual = esWriter.buildWriterResponse(ImmutableList.of(tuple1), response);

    assertEquals("Response should have one error and zero successes", expected, actual);
  }

  @Test
  public void testTwoSameFailure() throws Exception {
    Tuple tuple1 = mock(Tuple.class);
    Tuple tuple2 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);

    Exception e = new IllegalStateException();

    BulkItemResponse itemResponse = buildBulkItemFailure(e);
    BulkItemResponse itemResponse2 = buildBulkItemFailure(e);

    when(response.iterator()).thenReturn(ImmutableList.of(itemResponse, itemResponse2).iterator());

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addError(e, tuple1);
    expected.addError(e, tuple2);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual =
        esWriter.buildWriterResponse(ImmutableList.of(tuple1, tuple2), response);

    assertEquals("Response should have two errors and no successes", expected, actual);

    // Ensure the errors actually get collapsed together
    Map<Throwable, Collection<Tuple>> actualErrors = actual.getErrors();
    HashMap<Throwable, Collection<Tuple>> expectedErrors = new HashMap<>();
    expectedErrors.put(e, ImmutableList.of(tuple1, tuple2));
    assertEquals("Errors should have collapsed together", expectedErrors, actualErrors);
  }

  @Test
  public void testTwoDifferentFailure() throws Exception {
    Tuple tuple1 = mock(Tuple.class);
    Tuple tuple2 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);

    Exception e = new IllegalStateException("Cause");
    Exception e2 = new IllegalStateException("Different Cause");
    BulkItemResponse itemResponse = buildBulkItemFailure(e);
    BulkItemResponse itemResponse2 = buildBulkItemFailure(e2);

    when(response.iterator()).thenReturn(ImmutableList.of(itemResponse, itemResponse2).iterator());

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addError(e, tuple1);
    expected.addError(e2, tuple2);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual =
        esWriter.buildWriterResponse(ImmutableList.of(tuple1, tuple2), response);

    assertEquals("Response should have two errors and no successes", expected, actual);

    // Ensure the errors did not get collapsed together
    Map<Throwable, Collection<Tuple>> actualErrors = actual.getErrors();
    HashMap<Throwable, Collection<Tuple>> expectedErrors = new HashMap<>();
    expectedErrors.put(e, ImmutableList.of(tuple1));
    expectedErrors.put(e2, ImmutableList.of(tuple2));
    assertEquals("Errors should not have collapsed together", expectedErrors, actualErrors);
  }

  @Test
  public void testSuccessAndFailure() throws Exception {
    Tuple tuple1 = mock(Tuple.class);
    Tuple tuple2 = mock(Tuple.class);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);

    Exception e = new IllegalStateException("Cause");
    BulkItemResponse itemResponse = buildBulkItemFailure(e);

    BulkItemResponse itemResponse2 = mock(BulkItemResponse.class);
    when(itemResponse2.isFailed()).thenReturn(false);

    when(response.iterator()).thenReturn(ImmutableList.of(itemResponse, itemResponse2).iterator());

    BulkWriterResponse expected = new BulkWriterResponse();
    expected.addError(e, tuple1);
    expected.addSuccess(tuple2);

    ElasticsearchWriter esWriter = new ElasticsearchWriter();
    BulkWriterResponse actual =
        esWriter.buildWriterResponse(ImmutableList.of(tuple1, tuple2), response);

    assertEquals("Response should have one error and one success", expected, actual);
  }

  private BulkItemResponse buildBulkItemFailure(Exception e) {
    BulkItemResponse itemResponse = mock(BulkItemResponse.class);
    when(itemResponse.isFailed()).thenReturn(true);
    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(itemResponse.getFailure()).thenReturn(failure);
    when(failure.getCause()).thenReturn(e);
    return itemResponse;
  }
}
