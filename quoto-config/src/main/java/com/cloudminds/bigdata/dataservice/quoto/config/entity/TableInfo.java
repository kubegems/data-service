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
	private String table_name;
	private String table_alias;
	private int business_id_three_level;
	private String business_name_three_level;
	private int business_id_two_level;
	private String business_name_two_level;
	private int business_id_one_level;
	private String business_name_one_level;
}
