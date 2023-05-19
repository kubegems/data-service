package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

import java.util.List;

@Data
public class JobBloodLineReq {
    private List<HueDataSource> hueDataSourceSrc;
    private List<HueDataSource> hueDataSourceDest;
    private String name;
    private String type;
    private String creator;
    private boolean realTimeTask;
    private boolean scheduleTask;
    private String lastRunTime;
    private String description;
    private String id;
}
