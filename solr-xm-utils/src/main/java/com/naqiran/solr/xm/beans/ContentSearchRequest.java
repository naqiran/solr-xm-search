package com.naqiran.solr.xm.beans;

import java.util.Date;

import lombok.Data;

@Data
public class ContentSearchRequest {
    private String nid;
    private String searchTerm;
    private String searchType;
    private String channel;
    private Date previewDate;
    private String contentType;
}