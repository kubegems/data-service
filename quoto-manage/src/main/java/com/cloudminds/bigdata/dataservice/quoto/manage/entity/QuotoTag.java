package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class QuotoTag extends BaseEntity{
    private int id;
    private int tag_id;
    private int quoto_id;
}
