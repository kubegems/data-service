package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Classify extends BaseEntity{
    private int id;
    private int pid=0;
    private String name;
    private int type=1;
}
