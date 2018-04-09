package org.apache.metron.solr.dao;

import org.apache.metron.indexing.dao.RetrieveLatestDao;
import org.apache.metron.indexing.dao.search.GetRequest;
import org.apache.metron.indexing.dao.update.Document;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SolrRetrieveLatestDao implements RetrieveLatestDao {

  private transient SolrClient client;

  public SolrRetrieveLatestDao(SolrClient client) {
    this.client = client;
  }

  @Override
  public Document getLatest(String guid, String collection) throws IOException {
    try {
      SolrDocument solrDocument = client.getById(collection, guid);
      return SolrUtilities.toDocument(solrDocument);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Document> getAllLatest(List<GetRequest> getRequests) throws IOException {
    Map<String, Collection<String>> collectionIdMap = new HashMap<>();
    for (GetRequest getRequest : getRequests) {
      Collection<String> ids = collectionIdMap
              .getOrDefault(getRequest.getSensorType(), new HashSet<>());
      ids.add(getRequest.getGuid());
      collectionIdMap.put(getRequest.getSensorType(), ids);
    }
    try {
      List<Document> documents = new ArrayList<>();
      for (String collection : collectionIdMap.keySet()) {
        SolrDocumentList solrDocumentList = client.getById(collectionIdMap.get(collection),
                new SolrQuery().set("collection", collection));
        documents.addAll(
                solrDocumentList.stream().map(SolrUtilities::toDocument).collect(Collectors.toList()));
      }
      return documents;
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }
}
