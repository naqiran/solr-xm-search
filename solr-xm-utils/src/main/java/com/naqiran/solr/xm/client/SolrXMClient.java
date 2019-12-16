package com.naqiran.solr.xm.client;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;

import java.util.Arrays;

public class SolrXMClient {
    public static SolrClient getIndexerClient() {
        return new ConcurrentUpdateSolrClient.Builder("localhost:8983").build();
    }

    public static SolrClient getQueryClient() {
        return new CloudHttp2SolrClient.Builder(Arrays.asList("localhost:8983")).build();
    }
}
