package com.naqiran.solr.xm.beans;

import lombok.Data;

@Data
public class SiteLocation {
    private String nid;
    private boolean exactLocation;
    private String locationId;
    private String priority;
    private String locationType;

}
