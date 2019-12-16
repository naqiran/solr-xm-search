package com.naqiran.solr.xm.indexer;

import com.naqiran.solr.xm.SearchConstants;
import com.naqiran.solr.xm.XMUtils;
import com.naqiran.solr.xm.beans.SearchContent;
import com.naqiran.solr.xm.beans.SearchTerms;
import com.naqiran.solr.xm.beans.SiteLocation;
import com.naqiran.solr.xm.client.SolrXMClient;
import lombok.extern.slf4j.Slf4j;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;

@Slf4j
public class SolrXMIndexer {

    public boolean index(final SearchContent content, final String eventType) {

        boolean response = false;
        final String location = content.getLocation();
        SolrClient client = null;
        if (StringUtils.isNotBlank(location)) {
            try {
                client = SolrXMClient.getIndexerClient();
                if (SearchConstants.DELETE_EVENT.equals(eventType)) {
                    client.deleteByQuery(SearchConstants.LOCATION_FIELD + ":*" + location + "*");
                    client.commit();
                    response = true;
                    log.info("Deleted the node: {}"+ location);
                }
                else {
                    final List<SolrInputDocument> documents = prepareDocument(content);
                    if (CollectionUtils.isNotEmpty(documents)) {
                        client.deleteByQuery(SearchConstants.LOCATION_FIELD + ":*" + location + "*");
                        client.add(documents);
                        client.commit();
                        response = true;
                        log.info("Indexing the document: {}");
                    } else {
                        log.error("Nothing to Index");
                    }
                }
            } catch (Exception indexingException) {
                log.error("Error Occured in Indexing the document: {} : {}", content.getLocation(), indexingException);
                try {
                    if (client != null) {
                        client.rollback();
                    }
                } catch (Exception rollbackException) {
                    log.error("Exception in RollingBack: {} : {}" + content.getLocation(), rollbackException);
                }
            }
        }
        return response;
    }

    public List<SolrInputDocument> prepareDocument(final SearchContent content) {
        List<SolrInputDocument> documents = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(content.getTriggers())) {
            for (final SiteLocation location : content.getTriggers()) {
                SolrInputDocument document = new SolrInputDocument();
                final String nid = location.getNid();
                final List<String> categories = XMUtils.getCategoryDval(nid);
                final List<String> facets = XMUtils.getFacetDval(nid);
                document.setField(SearchConstants.ID_FIELD, UUID.randomUUID().toString());
                document.setField(SearchConstants.NID_FIELD, nid);
                if (CollectionUtils.isNotEmpty(categories)) {
                    categories.stream().forEach(val -> document.addField(SearchConstants.CATEGORIES_FIELD, val));
                    document.setField(SearchConstants.CATEGORY_COUNT_FIELD, categories.size());
                }
                else {
                    document.setField(SearchConstants.CATEGORY_COUNT_FIELD, 0);
                }
                if (CollectionUtils.isNotEmpty(facets)) {
                    facets.stream().forEach(facet -> document.addField(SearchConstants.FACETS_FIELD, facet));
                    document.setField(SearchConstants.FACET_COUNT_FIELD, facets.size());
                }
                else {
                    document.setField(SearchConstants.FACET_COUNT_FIELD, 0);
                }

                document.setField(SearchConstants.EXACT_LOCATION_FIELD, location.isExactLocation());
                document.setField(SearchConstants.LOCATION_ID_FIELD, location.getLocationId());
                document.setField(SearchConstants.LOCATION_TYPE_FIELD, location.getLocationType());
                document.setField(SearchConstants.PRIORITY_FIELD, location.getPriority());
                addBasicFields(content, document);
                documents.add(document);
            }
        }
        if (CollectionUtils.isNotEmpty(content.getSearchTerms())) {
            for (final SearchTerms searchTerm : content.getSearchTerms()) {
                final SolrInputDocument document = new SolrInputDocument();
                addBasicFields(content, document);
                document.setField(SearchConstants.ID_FIELD, UUID.randomUUID().toString());
                document.setField(SearchConstants.SEARCH_TERMS_FIELD, searchTerm.getTerms());
                document.setField(SearchConstants.MATCH_MODE_FIELD, searchTerm.getMatches());
                document.setField(SearchConstants.LOCATION_ID_FIELD, searchTerm.getLocationId());
                document.setField(SearchConstants.LOCATION_TYPE_FIELD, searchTerm.getLocationType());
                document.setField(SearchConstants.NID_FIELD, searchTerm.getNid());
                document.setField(SearchConstants.PRIORITY_FIELD, searchTerm.getSearchTermPriority());
                final List<String> facets = XMUtils.getSearchTermFacetDval(searchTerm.getNid());
                if (CollectionUtils.isNotEmpty(facets)) {
                    facets.stream().forEach(facet -> document.addField(SearchConstants.FACETS_FIELD, facet));
                    document.setField(SearchConstants.FACET_COUNT_FIELD, facets.size());
                }
                else {
                    document.setField(SearchConstants.FACET_COUNT_FIELD, 0);
                }
                document.addField(SearchConstants.EXACT_LOCATION_FIELD,searchTerm.isExactLocation());

                documents.add(document);
            }
        }
        return documents;
    }

    private void addBasicFields(final SearchContent content, final SolrInputDocument document) {
        addDateField(document, SearchConstants.START_TIME_FIELD, content.getStartTime());
        addDateField(document, SearchConstants.END_TIME_FIELD, content.getEndTime());
        addDateField(document, SearchConstants.LAST_MODIFIED_TIME_FIELD, content.getLastModifiedDate());
        document.setField(SearchConstants.LAST_MODIFIED_BY_FIELD, content.getLastModifiedBy());
        document.setField(SearchConstants.STATE_FIELD, content.getStatus());
        document.setField(SearchConstants.LOCATION_FIELD, content.getLocation());
        document.setField(SearchConstants.TYPE_FIELD, content.getType());
        document.setField(SearchConstants.CONTENT_ID_FIELD, content.getNodeId());
        document.setField(SearchConstants.DEFAULT_CONTENT, content.isDefaultContent());
        document.setField(SearchConstants.DEFAULT_TYPE_FIELD, content.getDefaultType());
        if (CollectionUtils.isNotEmpty(content.getChannels())) {
            content.getChannels().stream().forEach(channel -> document.addField(SearchConstants.CHANNEL_FIELD, channel));
        }
    }

    private void addDateField(final SolrInputDocument document, final String fieldName, final Date date) {
        SimpleDateFormat solrDateFormatter = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss'Z'");
        solrDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (date != null) {
            document.addField(fieldName, solrDateFormatter.format(date));
        }
    }
}