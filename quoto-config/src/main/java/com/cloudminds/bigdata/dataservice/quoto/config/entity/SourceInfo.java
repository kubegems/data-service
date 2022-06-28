package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class SourceInfo {
    private String sourceName;
    private long db;
    private long table;
    private long totalFileSize;
}
