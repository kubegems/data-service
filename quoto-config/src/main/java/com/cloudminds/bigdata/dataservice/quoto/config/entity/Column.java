package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class Column {
    private String name;
    private String zh_name;
    private String type;
    private String type_detail;
    private int length;
    private String desc;
    private String default_value;
}
