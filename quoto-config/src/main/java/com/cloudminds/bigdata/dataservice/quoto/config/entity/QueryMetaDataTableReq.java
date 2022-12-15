package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class QueryMetaDataTableReq {
    private String database_name;
    private int table_type;
    private String model_level;
    private String data_domain;
    private int theme_id;
}
