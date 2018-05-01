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
package org.apache.metron.elasticsearch.writer;

import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.interfaces.FieldNameConverter;
import org.apache.metron.common.writer.BulkMessageWriter;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.elasticsearch.utils.ElasticsearchUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A bulk message writer for Elasticsearch.
 */
public class ElasticsearchWriter implements BulkMessageWriter<JSONObject>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);

  /**
   * The Elasticsearch transport client.
   */
  private transient TransportClient client;

  /**
   * The date formatter.
   */
  private SimpleDateFormat dateFormat;

  /**
   * Transforms the names of Elasticsearch fields.
   */
  private FieldNameConverter fieldNameConverter = new ElasticsearchFieldNameConverter();

  @Override
  public void init(Map stormConf, TopologyContext topologyContext, WriterConfiguration configurations) {
    Map<String, Object> globalConfiguration = configurations.getGlobalConfig();
    client = ElasticsearchUtils.getClient(globalConfiguration);
    dateFormat = ElasticsearchUtils.getIndexFormat(globalConfiguration);
  }

  @Override
  public BulkWriterResponse write(
          String sensorType,
          WriterConfiguration configurations,
          Iterable<Tuple> tuples,
          List<JSONObject> messages) throws Exception {

    final String indexPostfix = dateFormat.format(new Date());
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    for(JSONObject message: messages) {
      JSONObject esDoc = new JSONObject();

      for(Object k : message.keySet()){
        deDot(k.toString(), message, esDoc);
      }

      String indexName = ElasticsearchUtils.getIndexName(sensorType, indexPostfix, configurations);
      IndexRequestBuilder indexRequestBuilder = client
              .prepareIndex(indexName, sensorType + "_doc")
              .setSource(esDoc.toJSONString());

      // set the document's unique identifier
      String guid = (String) esDoc.get(Constants.GUID);
      if(guid != null) {
        indexRequestBuilder.setId(guid);
      }

      // set the document timestamp
      Object ts = esDoc.get("timestamp");
      if(ts != null) {
        indexRequestBuilder = indexRequestBuilder.setTimestamp(ts.toString());
      }

      LOG.debug("Adding write to bulk request; index={}, sensor={}", indexName, sensorType);
      bulkRequest.add(indexRequestBuilder);
    }

    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    return buildWriteResponse(tuples, bulkResponse);
  }

  @Override
  public String getName() {
    return "elasticsearch";
  }

  protected BulkWriterResponse buildWriteResponse(Iterable<Tuple> tuples, BulkResponse bulkResponse) throws Exception {

    // Elasticsearch responses are in the same order as the request, giving us an implicit mapping with Tuples
    BulkWriterResponse writerResponse = new BulkWriterResponse();
    if (bulkResponse.hasFailures()) {

      LOG.debug("Elasticsearch write has some errors; {} document(s) took {} ms; failure(s)={}",
              bulkResponse.getItems().length, bulkResponse.getTook().millis(), bulkResponse.buildFailureMessage());

      Iterator<BulkItemResponse> respIter = bulkResponse.iterator();
      Iterator<Tuple> tupleIter = tuples.iterator();
      while (respIter.hasNext() && tupleIter.hasNext()) {
        BulkItemResponse item = respIter.next();
        Tuple tuple = tupleIter.next();

        if (item.isFailed()) {
          Exception cause = item.getFailure().getCause();
          writerResponse.addError(cause, tuple);

        } else {
          writerResponse.addSuccess(tuple);
        }

        // Should never happen, so fail the entire batch if it does.
        if (respIter.hasNext() != tupleIter.hasNext()) {
          throw new Exception(bulkResponse.buildFailureMessage());
        }
      }
    } else {

      LOG.trace("Elasticsearch write success; {} document(s) took {} ms",
              bulkResponse.getItems().length, bulkResponse.getTook().millis());
      writerResponse.addAllSuccesses(tuples);
    }

    return writerResponse;
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  //JSONObject doesn't expose map generics
  @SuppressWarnings("unchecked")
  private void deDot(String field, JSONObject origMessage, JSONObject message){

    if(field.contains(".")){
      LOG.debug("Dotted field: {}", field);
    }

    String newKey = fieldNameConverter.convert(field);
    message.put(newKey,origMessage.get(field));
  }

}

