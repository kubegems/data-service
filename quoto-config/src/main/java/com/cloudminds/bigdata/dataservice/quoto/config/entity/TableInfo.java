package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class TableInfo extends BaseEntity{
	private int id;
	private int database_id;
	private Integer theme_id;
	private String theme_name;
	private int business_id;
	private String business_name;
	private int pid_business_id;
	private int pid_business_name;
	private String table_name;
	private String table_alias;
}
