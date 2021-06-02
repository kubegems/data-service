package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Dimension extends BaseEntity {
	private int id;
	private String name;
	private String code;
	private String alias;
	private String column_alias;
}
