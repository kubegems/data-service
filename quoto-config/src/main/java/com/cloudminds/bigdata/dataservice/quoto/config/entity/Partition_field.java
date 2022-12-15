package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class Partition_field {
    private String name;
    private String type;
    private int length;
    private String format;
    private String desc;
}
