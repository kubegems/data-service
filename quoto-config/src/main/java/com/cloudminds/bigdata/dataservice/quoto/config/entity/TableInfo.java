package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class TableInfo extends BaseEntity{
	private int id;
	private int database_id;
	private Integer business_process_id;
	private String business_process_name;
	private String data_domain_name;
	private String business_name;
	private String table_name;
	private String table_alias;
}
