package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class Partition_field {
    private String name;
    private String type;
    private int length;
    private String format;
    private String type_detail;
    private String desc;
    private String default_value;
}
