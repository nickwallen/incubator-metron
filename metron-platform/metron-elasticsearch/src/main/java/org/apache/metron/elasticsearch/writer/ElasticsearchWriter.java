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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.interfaces.FieldNameConverter;
import org.apache.metron.common.interfaces.FieldNameConverters;
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
   * Allows the user to specify the {@link FieldNameConverter} to use.
   */
  private static final String FIELD_NAME_CONVERTER_KEY = "indexing.writer.field.name.converter";

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
    fieldNameConverter = null;
  }

  /**
   * Writes messages to an Elasticsearch index.
   *
   * @param sensorType The type of sensor generating the messages.
   * @param writerConfig Configurations that should be passed to the writer.
   * @param tuples The Tuples that produced the message to be written.
   * @param messages The messages that need written to an Elasticsearch index.
   * @return The response received after indexing all of the messages.
   * @throws Exception
   */
  @Override
  public BulkWriterResponse write(String sensorType, WriterConfiguration writerConfig, Iterable<Tuple> tuples, List<JSONObject> messages) throws Exception {

    // initialize the field name converter?
    if(fieldNameConverter == null) {
      fieldNameConverter = createFieldNameConverter(sensorType, writerConfig);
    }

    // create the bulk index request
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    // create an index request for each message
    for(JSONObject message: messages) {
      IndexRequestBuilder request = createIndexRequest(sensorType, writerConfig, message);
      bulkRequest.add(request);
    }

    // submit the bulk request
    LOG.debug("Submitting bulk index request; sensorType={}, count={}", sensorType, messages.size());
    BulkResponse bulkResponse = bulkRequest
            .execute()
            .actionGet();

    // create the writer's response
    LOG.debug("Received response to bulk index request; sensorType={}, count={}, took={} ms, ",
            sensorType, ArrayUtils.getLength(bulkResponse.getItems()), bulkResponse.getTookInMillis());
    BulkWriterResponse response = buildWriterResponse(tuples, bulkResponse);
    return response;
  }

  /**
   * Creates a {@link FieldNameConverter} based on the writer's configuration.
   *
   * @param sensorType The sensor type.
   * @param writerConfig The writer's configuration.
   * @return An optional {@link FieldNameConverter}.
   */
  private Optional<FieldNameConverter> createFieldNameConverter(String sensorType, WriterConfiguration writerConfig) {
    Optional<FieldNameConverter> converter = Optional.empty();

    Map<String, Object> sensorConfig = writerConfig.getSensorConfig(sensorType);
    if(sensorConfig.containsKey(FIELD_NAME_CONVERTER_KEY)) {

      String converterValue = (String) sensorConfig.get(FIELD_NAME_CONVERTER_KEY);
      if (StringUtils.isBlank(converterValue)) {

        // this assumes that one writer instance is used for each sensor type
        LOG.debug("Using field name converter '{}' for sensorType={}", converterValue, sensorType);
        converter = Optional.of(FieldNameConverters.valueOf(converterValue).get());
      }

    } else {
      LOG.debug("No field name converter defined; property={}", FIELD_NAME_CONVERTER_KEY);
    }

    return converter;
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

    // the document is essentially a copy of the message
    JSONObject document = new JSONObject();
    for (Object key : message.keySet()) {

      String fieldName = (String) key;
      Object fieldValue = message.get(fieldName);

      // allow fields to be renamed in the document
      if(fieldNameConverter.isPresent()) {
        fieldName = fieldNameConverter.get().convert(fieldName);
      }

      document.put(fieldName, fieldValue);
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
    BulkWriterResponse writerResponse;
    if (bulkResponse.hasFailures()) {
      writerResponse = handleSomeFailures(tuples, bulkResponse);

    } else {
      writerResponse = handleSuccess(tuples);
    }

    if(writerResponse.numberOfErrors() > writerResponse.numberOfSuccesses()) {
      LOG.warn("Writer response created; successes={}, errors={}, cause={}",
              writerResponse.numberOfSuccesses(), writerResponse.numberOfErrors(), bulkResponse.buildFailureMessage());

    } else {
      LOG.debug("Writer response created; successes={}, errors={}, cause={}",
              writerResponse.numberOfSuccesses(), writerResponse.numberOfErrors(), bulkResponse.buildFailureMessage());
    }

    return writerResponse;
  }

  /**
   * Handles an Elasticsearch resposne that contains no failures.
   *
   * @param tuples The tuples whose messages were written.
   * @return The {@link BulkWriterResponse}.
   */
  private BulkWriterResponse handleSuccess(Iterable<Tuple> tuples) {

    BulkWriterResponse writerResponse = new BulkWriterResponse();
    writerResponse.addAllSuccesses(tuples);
    return writerResponse;
  }

  /**
   * Handles an Elasticsearch response that contains some successes and some failures.
   *
   * @param tuples The tuples whose messages were written.
   * @param bulkResponse The Elasticsearch response.
   * @return The {@link BulkWriterResponse}.
   */
  private BulkWriterResponse handleSomeFailures(Iterable<Tuple> tuples, BulkResponse bulkResponse) {

    BulkWriterResponse writerResponse = new BulkWriterResponse();

    // iterate through each item to distinguish between successes and failures
    Iterator<BulkItemResponse> respIter = bulkResponse.iterator();
    Iterator<Tuple> tupleIter = tuples.iterator();
    while (respIter.hasNext() && tupleIter.hasNext()) {

      BulkItemResponse item = respIter.next();
      Tuple tuple = tupleIter.next();

      if (item.isFailed()) {
        writerResponse.addError(item.getFailure().getCause(), tuple);

      } else {
        writerResponse.addSuccess(tuple);
      }

      // should never happen, so fail the entire batch if it does.
      if (respIter.hasNext() != tupleIter.hasNext()) {
        throw new IllegalStateException(bulkResponse.buildFailureMessage());
      }
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

