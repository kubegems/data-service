package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class TableAccessInfo {
    private int id;
    private String table_alias;
    private String table_des;
    private String db_des;
    private String service_path;
}
