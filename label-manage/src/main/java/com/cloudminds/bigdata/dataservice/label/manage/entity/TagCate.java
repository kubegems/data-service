package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TagCate extends BaseEntity{
    private String tag_cate_id;
    private String pid;
    private String tag_cate_name;
    private int tag_object_id;
}
