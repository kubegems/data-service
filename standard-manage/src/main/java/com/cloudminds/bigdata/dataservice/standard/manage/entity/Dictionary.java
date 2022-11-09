package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = false)
@Data
public class Dictionary extends BaseEntity{
    private int id;
    private String zh_name;
    private String en_name;
    private String code;
    private List<DictionaryValue> fields;
    private int state;
    private int classify_id;
}
