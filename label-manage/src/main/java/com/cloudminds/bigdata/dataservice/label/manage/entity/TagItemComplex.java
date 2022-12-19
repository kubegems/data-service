package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagItemComplex extends BaseEntity{
    private int id;
    private String name;
    private int tag_object_id;
    private String filter;
    private String tag_items;
    private int state;
}
