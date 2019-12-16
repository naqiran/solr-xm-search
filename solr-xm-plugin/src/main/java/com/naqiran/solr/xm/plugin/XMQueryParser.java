package com.naqiran.solr.xm.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.LuceneQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class XMQueryParser extends LuceneQParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(XMQueryParser.class);

    private static final String CATEGORY_ATTRIBUTE = "categories";
    private static final String FACET_ATTRIBUTE = "facets";
    private static final String START_TIME_ATTRIBUTE = "startTime";
    private static final String END_TIME_ATTRIBUTE = "endTime";
    private static final String CONTENT_TYPE_ATTRIBUTE = "type";
    private static final String CONTENT_STATE_ATTRIBUTE = "state";
    private static final String FACET_COUNT_ATTRIBUTE = "facetCount";
    private static final String CATEGORY_COUNT_ATTRIBUTE = "categoryCount";
    private static final String EXACT_LOCATION_ATTRIBUTE = "exactLocation";
    private static final String SEARCH_TERMS_ATTRIBUTE = "searchTerms";
    private static final String SEARCH_TERMS_COMBINED_ATTRIBUTE = "searchTermsString";
    private static final String MATCH_MODE_ATTRIBUTE = "matchMode";
    private static final String CHANNELS_ATTRIBUTE = "channels";
    private static final String DEFAULT_ATTRIBUTE = "defaultContent";
    private static final String N_EXPRESSION_ATTRIBUTE = "nid";
    private static final String DEFAULT_TYPE_ATTRIBUTE = "defaultType";

    private static final String CONTENT_TYPE_PARAMETER = "contentType";
    private static final String PREVIEW_DATE_PARAMETER = "previewDate";
    private static final String CHANNEL_PARAMETER = "channel";
    private static final String SEARCH_TYPE_PARAMETER = "searchType";

    private static final String FUTURE_EPOCH = "1DAY";


    private static final String TERMS_SEARCH_TYPE = "terms";
    private static final String DEFAULT_SEARCH_TYPE = "default";
    private static final String STATE_ACTIVE = "ACTIVE";

    private static final String AND = " AND ";
    private static final String AND_NOT = " AND NOT ";
    private static final String OR = " OR ";
    private static final String SEPARATOR = ":";

    private List<String> categoryDvals = null;
    private List<String> facetDvals = null;
    private String contentType = null;
    private String dateTime = null;
    private String startTime = null;
    private String queryString = null;
    private List<String> termsList = null;
    private String channel = null;
    private String searchType = null;
    private String nid = null;
    private boolean isPreview;

    public XMQueryParser(final String query, final SolrParams localParams, final SolrParams solrParams, final SolrQueryRequest request) {
        super(query, localParams, solrParams, request);
        this.queryString = query;
        extractRuleQueryParam(localParams);
    }

    public void extractRuleQueryParam(final SolrParams localParam) {
        if (localParam != null) {
            nid = localParam.get(N_EXPRESSION_ATTRIBUTE);
            contentType = localParam.get(CONTENT_TYPE_PARAMETER);
            dateTime = "NOW";
            startTime = "NOW";
            if (StringUtils.isNotBlank(localParam.get(PREVIEW_DATE_PARAMETER))) {
                isPreview = true;
                dateTime = localParam.get(PREVIEW_DATE_PARAMETER);
            }
            //Get future dated content to compute the TTL - Currently we are assuming a Day for computation.
            startTime = dateTime + "+" + FUTURE_EPOCH;
            channel = localParam.get(CHANNEL_PARAMETER);
            searchType = localParam.get(SEARCH_TYPE_PARAMETER);
        }
        if (TERMS_SEARCH_TYPE.equalsIgnoreCase(searchType) && StringUtils.isNotBlank(queryString)) {
            termsList = Arrays.asList(queryString.replaceAll("\\W+", " ").split("\\s+"));
            facetDvals = getSearchTermFacetDval(nid);
        } else {
            categoryDvals = getCategoryDval(nid);
            facetDvals = getFacetDval(nid);
        }
    }

    @Override
    public Query parse() throws SyntaxError {
        return getFullQuery();
    }

    public Query getFullQuery() throws SyntaxError {
        final BooleanQuery.Builder fullQueryBuilder = new BooleanQuery.Builder();
        if (TERMS_SEARCH_TYPE.equalsIgnoreCase(searchType)) {
            if (StringUtils.isBlank(queryString)) {
                throw new SyntaxError("Search Term should be present for Term Search Query");
            }
            qstr = getSearchTerms();
            final Query mainQuery = super.parse();
            fullQueryBuilder.add(mainQuery, Occur.MUST);
            addBoostFunctions(fullQueryBuilder);
        } else if (DEFAULT_SEARCH_TYPE.equalsIgnoreCase(searchType)) {
            if (StringUtils.isBlank(nid)) {
                throw new SyntaxError("Default Query Expects ID!");
            }
            qstr = getDefaultQuery();
            final Query mainQuery = super.parse();
            fullQueryBuilder.add(mainQuery, Occur.MUST);
        } else {
            if (CollectionUtils.isEmpty(categoryDvals)) {
                throw new SyntaxError("Query expects the Category ID!");
            }
            qstr = getCategoryQuery();
            final Query mainQuery = super.parse();
            fullQueryBuilder.add(mainQuery, Occur.MUST);
            addBoostFunctions(fullQueryBuilder);
        }
        Query fullQuery = fullQueryBuilder.build();
        LOGGER.info("Full Query: {}", fullQuery);
        return fullQuery;
    }

    private String getDefaultQuery() {
        final StringBuilder query = new StringBuilder();
        wrapStart(query, null);
        wrapStart(query, null);
        if (StringUtils.contains(nid, "D")) {
            appendAttribute(query, N_EXPRESSION_ATTRIBUTE, "N-" + StringUtils.substringAfter(nid, "D"), null);
            appendAttribute(query, N_EXPRESSION_ATTRIBUTE, nid, OR);
        } else {
            appendAttribute(query, N_EXPRESSION_ATTRIBUTE, nid, null);
            appendAttribute(query, N_EXPRESSION_ATTRIBUTE, StringUtils.replace(nid, "N-", "N-*D"), OR);
        }
        wrapEnd(query, null);
        applyRestriction(query);
        wrapEnd(query, null);
        addDefaultContent(query);
        return query.toString();
    }

    private String getCategoryQuery() {
        final StringBuilder query = new StringBuilder();
        final String facetQuery = expandAttribute(FACET_ATTRIBUTE, facetDvals, OR, true);
        final String fullCategoryQuery = expandAttribute(CATEGORY_ATTRIBUTE, categoryDvals, AND, true);
        wrapStart(query, null);
        wrapStart(query, null);
        query.append(fullCategoryQuery);

        if (categoryDvals.size() > 1) {
            wrapStart(query, OR);
            appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(1), null);
            wrapEnd(query,"10");

            wrapStart(query, OR);
            appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(0), null);
            appendAttribute(query, EXACT_LOCATION_ATTRIBUTE, "true", AND_NOT);
            wrapEnd(query,"5");
        }
        if (StringUtils.isNotBlank(facetQuery)) {
            if (categoryDvals.size() > 1) {
                wrapStart(query, OR);
                appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(0), null);
                appendAttribute(query, EXACT_LOCATION_ATTRIBUTE, "true", AND_NOT);
                query.append(AND).append(facetQuery);
                wrapEnd(query, null);

                wrapStart(query, OR);
                appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(1), null);
                query.append(AND).append(facetQuery);
                wrapEnd(query, "10");

                wrapStart(query, OR);
                appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(1), null);
                appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(0), AND);
                query.append(AND).append(facetQuery);
                wrapEnd(query, "20");
            } else {
                wrapStart(query, OR);
                appendAttribute(query, CATEGORY_ATTRIBUTE, categoryDvals.get(0), null);
                query.append(AND).append(facetQuery);
                wrapEnd(query, null);
            }
            wrapEnd(query, null);
        }
        else {
            wrapEnd(query, null);
            appendAttribute(query, FACET_ATTRIBUTE, appendRange(null, null), AND_NOT);
        }
        applyRestriction(query);
        wrapEnd(query, null);
        addDefaultContent(query);
        LOGGER.info("Category Query: {}", query);
        return query.toString();
    }

    private String getSearchTerms() {
        final StringBuilder searchTermQuery = new StringBuilder();

        wrapStart(searchTermQuery, null);

        wrapStart(searchTermQuery, null);
        //Match Exact
        wrapStart(searchTermQuery, null);
        appendAttribute(searchTermQuery, MATCH_MODE_ATTRIBUTE, "MATCHEXACT", null);
        appendAttribute(searchTermQuery, SEARCH_TERMS_COMBINED_ATTRIBUTE, "\"" + queryString + "\"", AND);
        wrapEnd(searchTermQuery, "20");

        //Match Phrase
        wrapStart(searchTermQuery, OR);
        appendAttribute(searchTermQuery, MATCH_MODE_ATTRIBUTE, "MATCHPHRASE", null);
        appendAttribute(searchTermQuery, SEARCH_TERMS_ATTRIBUTE, "\"*" + queryString + "*\"", AND);
        wrapEnd(searchTermQuery, "10");

        //Match All
        wrapStart(searchTermQuery, OR);
        appendAttribute(searchTermQuery, MATCH_MODE_ATTRIBUTE, "MATCHALL", null);
        searchTermQuery.append(AND);
        searchTermQuery.append(expandAttribute(SEARCH_TERMS_ATTRIBUTE, termsList, AND, true));
        wrapEnd(searchTermQuery, "5");

        wrapEnd(searchTermQuery, null);
        //If facets exists check all the facets matches or else
        if (CollectionUtils.isEmpty(facetDvals)) {
            appendAttribute(searchTermQuery, FACET_ATTRIBUTE, appendRange(null, null), AND_NOT);
        }

        applyRestriction(searchTermQuery);
        wrapEnd(searchTermQuery, null);
        addDefaultContent(searchTermQuery);
        LOGGER.info("Terms Query: {}", searchTermQuery);
        return searchTermQuery.toString();
    }

    private void applyRestriction(final StringBuilder query) {
        //Only on preview check for activeness.
        if (!isPreview) {
            appendAttribute(query, CONTENT_STATE_ATTRIBUTE, STATE_ACTIVE, AND);
        }
        appendAttribute(query, START_TIME_ATTRIBUTE, appendRange(null,startTime), AND);
        appendAttribute(query, END_TIME_ATTRIBUTE, appendRange(dateTime, null), AND);
        appendAttribute(query, CHANNELS_ATTRIBUTE, channel, AND);
        appendAttribute(query, CONTENT_TYPE_ATTRIBUTE, contentType, AND);
    }

    private void addDefaultContent(final StringBuilder fullQuery) {
        wrapStart(fullQuery, OR);
        if (TERMS_SEARCH_TYPE.equalsIgnoreCase(searchType) && "gallery".equalsIgnoreCase(contentType)) {
            final String defaultType = "" + contentType + "-" + searchType;
            appendAttribute(fullQuery, DEFAULT_TYPE_ATTRIBUTE, defaultType, null);
        } else {
            appendAttribute(fullQuery, DEFAULT_ATTRIBUTE, "true", null);
        }
        applyRestriction(fullQuery);
        wrapEnd(fullQuery, "0.01");
    }

    private List<String> getCategoryDval(final String value) {
        final String categoryAndFacetDvalIds = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(categoryAndFacetDvalIds)) {
            final String onlyCategoryDvals = StringUtils.substringBefore(categoryAndFacetDvalIds, "Z");
            return splitAndDecode(onlyCategoryDvals, "D");
        }
        return Collections.emptyList();
    }

    private List<String> getFacetDval(final String value) {
        final String categoryAndFacetDvalIds = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(categoryAndFacetDvalIds)) {
            final String onlyCategoryDvals = StringUtils.substringAfter(categoryAndFacetDvalIds, "Z");
            return splitAndDecode(onlyCategoryDvals, "Z");
        }
        return Collections.emptyList();
    }

    private List<String> getSearchTermFacetDval(final String value) {
        final String facetDval = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(facetDval)) {
            return splitAndDecode(facetDval, "Z");
        }
        return Collections.emptyList();
    }

    private List<String> splitAndDecode(final String value, final String separator) {
        if (StringUtils.isNotBlank(value)) {
            return Stream.of(StringUtils.split(value, separator)).map(val -> String.valueOf(Long.parseLong(val, 36))).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    //Warning: Whitespace in the function query will break the entire function.
    protected void addBoostFunctions(final BooleanQuery.Builder query) throws SyntaxError {
        final List<String> responses = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(categoryDvals)) {
            final String categoryTermFrequency = categoryDvals.stream().map(facet -> "termfreq(" + CATEGORY_ATTRIBUTE + "," + facet + ")")
                    .collect(Collectors.joining(","));
            final String filterLessCategories = "if(lte(" + CATEGORY_COUNT_ATTRIBUTE + ",sum(" + categoryTermFrequency + ")),1,-10000)";
            LOGGER.info("Category Boost Query 1: {}", filterLessCategories);
            responses.add(filterLessCategories);
        }

        if (CollectionUtils.isNotEmpty(facetDvals)) {
            String facetTermFrequency = facetDvals.stream().map(facet -> "termfreq(" + FACET_ATTRIBUTE + "," + facet + ")")
                    .collect(Collectors.joining(","));

            String filterLessFacets = "if(lte(" + FACET_COUNT_ATTRIBUTE + ",sum(" + facetTermFrequency + ")),1,-10000)";
            String filterExactLocation = "if(" + EXACT_LOCATION_ATTRIBUTE + ",if(eq(" + FACET_COUNT_ATTRIBUTE + ","+ facetDvals.size() +"),if(eq(" + facetDvals.size()
                    + ",sum(" + facetTermFrequency + ")),1,-10000),if(gt(" + facetDvals.size() + ",0),-10000,0)),0)";
            responses.add(filterExactLocation);
            responses.add(filterLessFacets);
        }
        createBoostFunctions(query, responses);
    }

    protected void createBoostFunctions(final BooleanQuery.Builder query, final List<String> boostFunctions)  throws SyntaxError {
        if (CollectionUtils.isNotEmpty(boostFunctions)) {
            for (String boostFunc : boostFunctions) {
                if (StringUtils.isNotBlank(boostFunc)) {
                    final Map<String, Float> ff = SolrPluginUtils.parseFieldBoosts(boostFunc);
                    for (Entry<String, Float> entry : ff.entrySet()) {
                        Query fq = subQuery(entry.getKey(), FunctionQParserPlugin.NAME).getQuery();
                        Float b = entry.getValue();
                        if (null != b) {
                            fq = new BoostQuery(fq, b);
                        }
                        query.add(fq, Occur.MUST);
                    }
                }
            }
        }
    }

    private void appendAttribute(final StringBuilder query, final String attributeName, final String attributeValue, final String operator) {
        if (StringUtils.isNotBlank(attributeName) && StringUtils.isNotBlank(attributeValue)) {
            if (StringUtils.isNotBlank(operator)) {
                query.append(operator);
            }
            query.append(attributeName).append(SEPARATOR).append(attributeValue);
        }
    }

    private String appendRange(final String start, final String end) {
        return "[" + StringUtils.defaultIfBlank(start, "*") + " TO " + StringUtils.defaultIfBlank(end, "*") + "]";
    }

    private void wrapStart(final StringBuilder query, final String operator) {
        if (StringUtils.isNotBlank(operator)) {
            query.append(operator);
        }
        query.append("(");
    }

    private void wrapEnd(final StringBuilder query, final String boost) {
        query.append(")");
        if (StringUtils.isNotBlank(boost)) {
            query.append("^").append(boost);
        }
    }

    private String expandAttribute(final String attributeName, final List<String> values, final String operator, final boolean wrap) {
        String expandedAttribute = null;
        if (CollectionUtils.isNotEmpty(values) && StringUtils.isNotBlank(attributeName) && StringUtils.isNotBlank(operator)) {
            expandedAttribute = values.stream().map(token -> attributeName + ":" + token).collect(Collectors.joining(operator));
            if (wrap) {
                expandedAttribute = "(" + expandedAttribute + ")";
            }
        }
        return expandedAttribute;
    }
}
