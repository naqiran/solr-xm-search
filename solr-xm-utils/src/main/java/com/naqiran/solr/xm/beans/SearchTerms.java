package com.naqiran.solr.xm.beans;

import lombok.Data;

@Data
public class SearchTerms {
    private String terms;
    private String matches;
    private String locationId;
    private String locationType;
    private String categoryId;
    private String filterIds;
    private boolean exactLocation;
    private String nid;
    private Integer searchTermPriority;
}
