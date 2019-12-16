package com.naqiran.solr.xm;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

public class XMUtils {

    public static List<String> getCategoryDval(final String value) {
        final String categoryAndFacetDvalIds = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(categoryAndFacetDvalIds)) {
            final String onlyCategoryDvals = StringUtils.substringBefore(categoryAndFacetDvalIds, "Z");
            return splitAndDecode(onlyCategoryDvals, "D");
        }
        return null;
    }

    public static List<String> getFacetDval(final String value) {
        final String categoryAndFacetDvalIds = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(categoryAndFacetDvalIds)) {
            final String onlyCategoryDvals = StringUtils.substringAfter(categoryAndFacetDvalIds, "Z");
            return splitAndDecode(onlyCategoryDvals, "Z");
        }
        return null;
    }

    public static List<String> getSearchTermFacetDval(final String value) {
        final String facetDvals = StringUtils.substringAfter(value, "N-");
        if (StringUtils.isNotBlank(facetDvals)) {
            return splitAndDecode(facetDvals, "Z");
        }
        return null;
    }

    private static List<String> splitAndDecode(final String value, final String separator) {
        if (StringUtils.isNotBlank(value)) {
            return Stream.of(StringUtils.split(value, separator)).map(val -> String.valueOf(Long.parseLong(val, 36))).collect(Collectors.toList());
        }
        return null;
    }
}

