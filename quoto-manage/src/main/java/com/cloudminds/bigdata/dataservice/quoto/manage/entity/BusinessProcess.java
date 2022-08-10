package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class BusinessProcess extends BaseEntity {
	private int id;
	private int theme_id;
	private String name;
}
