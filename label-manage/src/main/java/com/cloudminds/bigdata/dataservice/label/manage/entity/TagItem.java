package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagItem extends BaseEntity{
    private String tag_id;
    private String tag_name;
    private String tag_cate_id;
    private int value_type;
    private String value_scope;
    private List<TagEnumValue> tagEnumValueList;
    private int state;
    private String tag_type;
    private boolean exclusive;
    private int source_type;
    private String source;
    private int update_cycle;
    private String update_cycle_unit;
    private String updater;
    private String tag_cate_one_id;
    private String tag_cate_one_name;
    private String tag_cate_two_id;
    private String tag_cate_two_name;
}
