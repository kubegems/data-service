package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class TableAccessTop {
    private String id;
    private String table_alias;
    private String table_name;
    private String des;
    private int total;
}
