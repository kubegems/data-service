package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class TableInfo extends BaseEntity{
	private int id;
	private int database_id;
	private String table;
	private String table_alias;
}
