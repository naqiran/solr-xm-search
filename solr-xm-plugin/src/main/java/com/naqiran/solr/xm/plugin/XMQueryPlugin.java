package com.naqiran.solr.xm.plugin;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class XMQueryPlugin extends QParserPlugin {

    public static final String NAME = "xmq";

    @Override
    public QParser createParser(String query, SolrParams localParams, SolrParams solrParams, SolrQueryRequest request) {
        return new XMQueryParser(query, localParams, solrParams, request);
    }
}
