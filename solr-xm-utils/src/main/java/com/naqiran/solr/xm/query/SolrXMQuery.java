package com.naqiran.solr.xm.query;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import com.naqiran.solr.xm.SearchConstants;
import com.naqiran.solr.xm.beans.ContentSearchRequest;
import com.naqiran.solr.xm.beans.SearchContent;
import com.naqiran.solr.xm.beans.SearchTerms;
import com.naqiran.solr.xm.beans.SiteLocation;
import com.naqiran.solr.xm.client.SolrXMClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.Arrays;
import java.util.Date;

@Slf4j

public class SolrXMQuery {

    public SearchContent queryRuleForId(final ContentSearchRequest contentSearchRequest) {
        try {
            if(StringUtils.isEmpty(contentSearchRequest.getSearchTerm()) && StringUtils.isEmpty(contentSearchRequest.getNid())){
                throw new IllegalArgumentException("Search Query/Term and Nid should not be empty");
            }
            final SolrClient client = SolrXMClient.getQueryClient();
            final SolrQuery solrQuery = new SolrQuery();
            solrQuery.add(SearchConstants.QUERY_PARAM, getRuleQuery(contentSearchRequest));
            solrQuery.setIncludeScore(true);
            //Change to include endtime for sorting the content
            if(SearchConstants.SEARCH_TERMS_TYPE.equalsIgnoreCase(contentSearchRequest.getSearchType()) && SearchConstants.GALLERY.equalsIgnoreCase(contentSearchRequest.getContentType())) {
                solrQuery.add(SearchConstants.SORT, "score desc, exactLocation desc, priority asc, endTime asc, sub(endTime,startTime) asc, lastModifiedTime desc");
            } else {
                solrQuery.add(SearchConstants.SORT, "score desc, exactLocation desc, endTime asc, sub(endTime,startTime) asc, lastModifiedTime desc");
            }
            log.info("Search query is "+solrQuery.getQuery());
            final QueryResponse response = client.query(solrQuery);
            final SolrDocumentList documents = response.getResults();
            return getSearchDocument(contentSearchRequest, documents);
        } catch (Exception e) {
            log.error("Error in getting the response {}", e);
        }
        return null;
    }

    /**
     * TTL is computed with epoch of current time and immediate starting content - If the content itself the prior content future contents are ignored
     * For the preview content TTL's will not be computed to avoid complexity - Don't send preview content to get the live content.
     * @param
     * @return SearchContent.
     */
    private SearchContent getSearchDocument(final ContentSearchRequest request, final SolrDocumentList responseDocuments) {
        SearchContent content = null;
        if (CollectionUtils.isNotEmpty(responseDocuments)) {
            SolrDocument selectedDocument = null;
            Date immediateDate = null;
            long ttl = -1;
            final Date currentDate = request.getPreviewDate() != null ? request.getPreviewDate() : new Date();
            for (final SolrDocument document: responseDocuments) {
                final Date startTime = (Date) document.get(SearchConstants.START_TIME_FIELD);
                final Date endTime = (Date) document.get(SearchConstants.END_TIME_FIELD);
                boolean futureContent = startTime.after(currentDate);
                if (futureContent && (immediateDate == null || (immediateDate != null && immediateDate.after(startTime)))) {
                    immediateDate = startTime;
                } else if (!futureContent){
                    if (request.getPreviewDate() == null) {
                        if (immediateDate != null && endTime.after(immediateDate)) {
                            ttl = immediateDate.getTime() - currentDate.getTime();
                        } else if (endTime.getTime() > currentDate.getTime()){
                            ttl = endTime.getTime() - currentDate.getTime();
                        }
                    }
                    selectedDocument = document;
                    break;
                }
            }

            float score = getScoreFromDocument(selectedDocument);

            if (score >= 0) {
                content = convertResponse(selectedDocument);
                content.setTtl(ttl);
            }
        }
        return content;
    }

    //I dont like this fix - but doing it for now. I will fix it later with the propery fix.
    private Float getScoreFromDocument(final SolrDocument document) {
        float score = 0;
        if (document != null && document.get("score") != null && ((Float)document.get("score")) > 0) {
            score = ((Float) document.get("score"));
        }
        return score;
    }

    private SearchContent convertResponse(final SolrDocument document) {
        SearchContent content = null;
        if (document != null) {
            content = new SearchContent();
            content.setLocation((String)document.get(SearchConstants.LOCATION_FIELD));
            content.setNodeId((String) document.get(SearchConstants.CONTENT_ID_FIELD));
            content.setDefaultContent((Boolean) document.get(SearchConstants.DEFAULT_CONTENT));
            if (document.get(SearchConstants.SEARCH_TERMS_FIELD) != null) {
                final SearchTerms searchTerm = new SearchTerms();
                searchTerm.setTerms((String) document.get(SearchConstants.SEARCH_TERMS_FIELD));
                searchTerm.setMatches((String) document.get(SearchConstants.MATCH_MODE_FIELD));
                if(null != document.get(SearchConstants.LOCATION_ID_FIELD)){
                    searchTerm.setLocationId((String) document.get(SearchConstants.LOCATION_ID_FIELD));
                }
                if(null != document.get(SearchConstants.LOCATION_TYPE_FIELD)){
                    searchTerm.setLocationType((String) document.get(SearchConstants.LOCATION_TYPE_FIELD));
                }
                content.setSearchTerms(Arrays.asList(searchTerm));
            }
            if (document.get(SearchConstants.NID_FIELD) != null) {
                final SiteLocation siteLocation = new SiteLocation();
                siteLocation.setNid((String) document.get(SearchConstants.NID_FIELD));
                siteLocation.setLocationId((String) document.get(SearchConstants.LOCATION_ID_FIELD));
                siteLocation.setLocationType((String) document.get(SearchConstants.LOCATION_TYPE_FIELD));
                siteLocation.setExactLocation((Boolean) document.get(SearchConstants.EXACT_LOCATION_FIELD));
                content.setTriggers(Arrays.asList(siteLocation));
            }
        }
        return content;
    }

    private String getRuleQuery(final ContentSearchRequest contentSearchRequest) {
        final StringBuilder query = new StringBuilder();
        SimpleDateFormat solrDateFormatter = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss'Z'");
        solrDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        query.append("{!xmq");
        appendQueryAttribute(query, "searchType", contentSearchRequest.getSearchType());
        appendQueryAttribute(query, "contentType",contentSearchRequest.getContentType());
        appendQueryAttribute(query, "nid", contentSearchRequest.getNid());
        appendQueryAttribute(query, "channel", contentSearchRequest.getChannel());
        if (contentSearchRequest.getPreviewDate() != null) {
            appendQueryAttribute(query, "previewDate", solrDateFormatter
                    .format(contentSearchRequest.getPreviewDate()));
        }
        query.append("}").append(ClientUtils.escapeQueryChars(StringUtils.defaultString(contentSearchRequest.getSearchTerm())));
        return query.toString();
    }

    private void appendQueryAttribute(final StringBuilder query, final String attributeName, final String attributeValue) {
        if (StringUtils.isNotBlank(attributeName) && StringUtils.isNotBlank(attributeValue)) {
            query.append(" ").append(attributeName).append("=").append(attributeValue);
        }
    }

}