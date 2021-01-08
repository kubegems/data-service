package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class DatabaseInfo extends BaseEntity{
	private int id;
	private String db_url;
	private String database;
}
