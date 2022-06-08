package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagEnumValue extends BaseEntity{
    private String tag_id;
    private String tag_value;
}
