package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class DbConnInfo {
	private String db_url;
	private String database;
	private String table_name;
	private String userName;
	private String password;
}
