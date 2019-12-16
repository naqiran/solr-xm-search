package com.naqiran.solr.xm.beans;

import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class SearchContent {
    private String nodeId;
    private String location;
    private String type;
    private Date startTime;
    private Date endTime;
    private Date lastModifiedDate;
    private String status;
    private List<SiteLocation> triggers;
    private List<SearchTerms> searchTerms;
    private List<String> channels;
    private long ttl;
    private boolean defaultContent;
    private Date lastModifiedTime;
    private String lastModifiedBy;
    private Integer priority;
    private String defaultType;
}