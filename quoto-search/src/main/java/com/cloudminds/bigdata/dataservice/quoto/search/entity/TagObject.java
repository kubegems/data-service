package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagObject extends BaseEntity{
    private int id;
    private String name;
    private String code;
    private String database;
    private String table;
    private String es_index;
}
