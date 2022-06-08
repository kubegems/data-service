package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagItem extends BaseEntity{
    private String tag_id;
    private String tag_name;
    private String tag_cate_id;
    private int value_type;
    private int state;
    private String tag_type;
    private boolean exclusive;
    private String id_type;
    private String entity_id;
}
