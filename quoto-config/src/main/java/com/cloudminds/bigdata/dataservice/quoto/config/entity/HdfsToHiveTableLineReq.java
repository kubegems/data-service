package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

import java.util.Set;

@Data
public class HdfsToHiveTableLineReq {
    private Set<String> hdfs;
    private String database;
    private String table;
}
