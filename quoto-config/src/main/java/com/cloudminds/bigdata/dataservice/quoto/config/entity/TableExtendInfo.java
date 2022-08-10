package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TableExtendInfo extends BaseEntity{
    private int id;
    private int database_id;
    private Integer theme_id;
    private String theme_name;
    private String business_name;
    private String table_name;
    private String table_alias;
    private int db_id;
    private String database;
    private String service_path;
    private String service_name;
}
