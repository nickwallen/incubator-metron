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

import org.apache.commons.lang.StringUtils;
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
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Writes messages to an Elasticsearch index.
 */
public class ElasticsearchWriter implements BulkMessageWriter<JSONObject>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The Elasticsearch client.
   */
  private transient TransportClient client;

  /**
   * An optional field name converter.
   */
  private Optional<FieldNameConverter> fieldNameConverter;

  /**
   * Formats the date contained in the index name.
   */
  private SimpleDateFormat dateFormat;

  @Override
  public void init(Map stormConf, TopologyContext topologyContext, WriterConfiguration writerConfig) {
    Map<String, Object> globalConfig = writerConfig.getGlobalConfig();
    client = ElasticsearchUtils.getClient(globalConfig);
    dateFormat = ElasticsearchUtils.getIndexFormat(globalConfig);

    // TODO build class based on configured class name?
    fieldNameConverter = Optional.of(new ElasticsearchFieldNameConverter());
  }


  /**
   * Writes messages to an Elasticsearch index.
   *
   * @param sensorType The type of sensor generating the messages.
   * @param configurations Configurations that should be passed to the writer.
   * @param tuples The Tuples that produced the message to be written.
   * @param messages The messages that need written to an Elasticsearch index.
   * @return The response received after indexing all of the messages.
   * @throws Exception
   */
  @Override
  public BulkWriterResponse write(String sensorType, WriterConfiguration configurations, Iterable<Tuple> tuples, List<JSONObject> messages) throws Exception {

    // create the bulk index request
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    // create an index request for each message
    for(JSONObject message: messages) {
      IndexRequestBuilder request = createIndexRequest(sensorType, configurations, message);
      bulkRequest.add(request);
    }

    // submit the bulk request
    LOG.debug("Submitting bulk index request; sensorType={}, count={}", sensorType, messages.size());
    BulkResponse bulkResponse = bulkRequest
            .execute()
            .actionGet();

    // create the response
    LOG.debug("Received response to bulk index request; took={} ms, ", bulkResponse.getTookInMillis());
    BulkWriterResponse response = buildWriterResponse(tuples, bulkResponse);
    return response;
  }

  /**
   * Creates a request to index a message.
   *
   * @param sensorType The type of sensor which generated the message.
   * @param writerConfig The user-defined configurations for this writer.
   * @param message The message that needs written to an Elasticsearch index.
   * @return The index request for the message.
   */
  private IndexRequestBuilder createIndexRequest(String sensorType, WriterConfiguration writerConfig, JSONObject message) {

    // create the document that will be indexed based on the message
    JSONObject document = createDocument(message);

    String indexName = indexName(sensorType, writerConfig);
    String docType = docType(sensorType);
    IndexRequestBuilder requestBuilder = client
            .prepareIndex(indexName, docType)
            .setSource(document.toJSONString());

    // set the document GUID
    String guid = (String) document.get(Constants.GUID);
    if(guid != null) {
      requestBuilder.setId(guid);
    }

    // set the document timestamp
    Object ts = document.get("timestamp");
    if(ts != null) {
      requestBuilder.setTimestamp(ts.toString());
    }

    return requestBuilder;
  }

  /**
   * Returns the name of the index.
   *
   * @param sensorType The type of sensor that generated the message.
   * @param writerConfig The user-defined indexing configuration.
   * @return The name of the index that the message should be written to.
   */
  public String indexName(String sensorType, WriterConfiguration writerConfig) {

    // use system time when generating the index name
    long now = System.currentTimeMillis();
    String postfix = dateFormat.format(new Date(now));
    String indexName = ElasticsearchUtils.getIndexName(sensorType, postfix, writerConfig);

    LOG.debug("Using index named '{}'; sensorType={}, timestamp={}", indexName, sensorType, now);
    return indexName;
  }

  /**
   * Returns the doc type.
   *
   * @param sensorType The type of sensor that generated the message.
   * @return The doc type for the message.
   */
  public String docType(String sensorType) {

    return sensorType + "_doc";
  }

  /**
   * Creates a representation of the document that will be indexed
   * based on a message.
   *
   * @param message The message that needs indexed.
   * @return That document that will be indexed.
   */
  private JSONObject createDocument(JSONObject message) {

    // make a deep copy of the message
    JSONObject document = new JSONObject();
    document.putAll(message);

    if(fieldNameConverter.isPresent()) {

      // allow the converter to rename any fields
      FieldNameConverter converter = fieldNameConverter.get();
      for (Object key : document.keySet()) {

        String originalField = (String) key;
        String newField = converter.convert(originalField);
        if (!StringUtils.equals(originalField, newField)) {

          // rename the field
          Object value = document.remove(originalField);
          document.put(newField, value);

          LOG.debug("Field renamed; original={}, new={}", originalField, newField);
        }
      }

    } else {
      LOG.debug("No field name converter present");
    }

    return document;
  }

  @Override
  public String getName() {
    return "elasticsearch";
  }

  /**
   * Builds the response based on the response recieved from Elasticsearch.
   *
   * @param tuples The tuples containing the messages that needed indexed.
   * @param bulkResponse The Elasticsearch response to the bulk request
   * @return The response.
   * @throws Exception
   */
  protected BulkWriterResponse buildWriterResponse(Iterable<Tuple> tuples, BulkResponse bulkResponse) throws Exception {

    // Elasticsearch responses are in the same order as the request, giving us an implicit mapping with Tuples
    BulkWriterResponse writerResponse = new BulkWriterResponse();
    if (bulkResponse.hasFailures()) {

      int failures = 0;
      Iterator<BulkItemResponse> respIter = bulkResponse.iterator();
      Iterator<Tuple> tupleIter = tuples.iterator();
      while (respIter.hasNext() && tupleIter.hasNext()) {

        BulkItemResponse item = respIter.next();
        Tuple tuple = tupleIter.next();

        if (item.isFailed()) {
          writerResponse.addError(item.getFailure().getCause(), tuple);
          failures++;

        } else {
          writerResponse.addSuccess(tuple);
        }

        // Should never happen, so fail the entire batch if it does.
        if (respIter.hasNext() != tupleIter.hasNext()) {
          throw new Exception(bulkResponse.buildFailureMessage());
        }
      }
      LOG.debug("Response contains {} failure(s) out of {} request(s); error={}",
              failures, bulkResponse.getItems().length, bulkResponse.buildFailureMessage());

    } else {
      writerResponse.addAllSuccesses(tuples);
    }


    return writerResponse;
  }

  @Override
  public void close() throws Exception {

    if(client != null) {
      client.close();
    }
  }
}

