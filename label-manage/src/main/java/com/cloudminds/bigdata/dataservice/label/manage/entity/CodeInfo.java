package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class CodeInfo extends BaseEntity{
    private int id;
    private String code;
    private String name;
}
