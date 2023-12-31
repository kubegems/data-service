package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class ColumnAlias extends BaseEntity {
	private int id;
	private int table_id;
	private String column_name;
	private String column_alias;
	private String data_type;
}
