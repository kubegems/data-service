package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class HueDataSource {
    private String type;
    private String host;
    private String port;
    private String model;
    private String env;
    private boolean joinDatahub;
    private String instanceName;
    private String database;
    private String table;
    private String topic;
    private String hdfsPath;
}
