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
package org.apache.metron.solr.dao;

import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.indexing.dao.AccessConfig;
import org.apache.metron.indexing.dao.RetrieveLatestDao;
import org.apache.metron.indexing.dao.search.SearchDao;
import org.apache.metron.indexing.dao.update.Document;
import org.apache.metron.indexing.dao.update.OriginalNotFoundException;
import org.apache.metron.indexing.dao.update.PatchRequest;
import org.apache.metron.indexing.dao.update.UpdateDao;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class SolrUpdateDao implements UpdateDao {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private transient SolrClient client;
  // TODO I don't know of a way to avoid knowing the collection.  Which means that
  private AccessConfig config;
  private SearchDao searchDao;
  private RetrieveLatestDao retrieveLatestDao;

  public SolrUpdateDao(SolrClient client, AccessConfig config, SearchDao searchDao, RetrieveLatestDao retrieveLatestDao) {
    this.client = client;
    this.config = config;
    this.searchDao = searchDao;
    this.retrieveLatestDao = retrieveLatestDao;
  }

  public SolrUpdateDao(SolrClient solrClient) {
    this.retrieveLatestDao = new SolrRetrieveLatestDao(solrClient)
  }

  @Override
  public void update(Document update, Optional<String> index) throws IOException {
    try {
      SolrInputDocument solrInputDocument = SolrUtilities.toSolrInputDocument(update);
      if (index.isPresent()) {
        this.client.add(index.get(), solrInputDocument);
      } else {
        this.client.add(solrInputDocument);
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void batchUpdate(Map<Document, Optional<String>> updates) throws IOException {
    // updates with a collection specified
    Map<String, Collection<SolrInputDocument>> solrCollectionUpdates = new HashMap<>();

    for (Entry<Document, Optional<String>> entry : updates.entrySet()) {
      SolrInputDocument solrInputDocument = SolrUtilities.toSolrInputDocument(entry.getKey());
      Optional<String> index = entry.getValue();
      if (index.isPresent()) {
        Collection<SolrInputDocument> solrInputDocuments = solrCollectionUpdates
            .getOrDefault(index.get(), new ArrayList<>());
        solrInputDocuments.add(solrInputDocument);
        solrCollectionUpdates.put(index.get(), solrInputDocuments);
      } else {
        String lookupIndex = config.getIndexSupplier().apply(entry.getKey().getSensorType());
        Collection<SolrInputDocument> solrInputDocuments = solrCollectionUpdates
            .getOrDefault(lookupIndex, new ArrayList<>());
        solrInputDocuments.add(solrInputDocument);
        solrCollectionUpdates.put(lookupIndex, solrInputDocuments);
      }
    }
    try {
      for (Entry<String, Collection<SolrInputDocument>> entry : solrCollectionUpdates.entrySet()) {
        this.client.add(entry.getKey(), entry.getValue());
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Document getPatchedDocument(PatchRequest request, Optional<Long> timestamp) throws OriginalNotFoundException, IOException {
    Map<String, Object> latest = request.getSource();
    if (latest == null) {
      Document latestDoc = retrieveLatestDao.getLatest(request.getGuid(), request.getSensorType());
      if (latestDoc != null && latestDoc.getDocument() != null) {
        latest = latestDoc.getDocument();
      } else {
        throw new OriginalNotFoundException(
            "Unable to patch an document that doesn't exist and isn't specified.");
      }
    }
    Map<String, Object> updated = JSONUtils.INSTANCE.applyPatch(request.getPatch(), latest);
    return new Document(
        updated,
        request.getGuid(),
        request.getSensorType(),
        timestamp.orElse(System.currentTimeMillis()));
  }

}
