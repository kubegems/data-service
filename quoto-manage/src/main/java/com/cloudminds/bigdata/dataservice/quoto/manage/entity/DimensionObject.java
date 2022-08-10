package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class DimensionObject extends BaseEntity{
    private int id;
    private String name;
    private String code;
    private int type;
}
