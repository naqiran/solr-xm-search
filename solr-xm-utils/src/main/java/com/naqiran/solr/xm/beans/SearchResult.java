package com.naqiran.solr.xm.beans;

import lombok.Data;

import java.util.Date;

@Data
public class SearchResult {
    String contentNodeId;
    String locationId;
    String locationType;
    String jcrPath;
    Date contenTtlDate;
}
