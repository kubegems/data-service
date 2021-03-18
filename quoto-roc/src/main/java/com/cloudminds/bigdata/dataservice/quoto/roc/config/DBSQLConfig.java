package com.cloudminds.bigdata.dataservice.quoto.roc.config;

import apijson.framework.APIJSONSQLConfig;

public class DBSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_CLICKHOUSE; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "harix"; // TODO 默认数据库名
	}
	

	public DBSQLConfig(String dbUrl, String dbAccount,String dbPassword, String configDbUrl,String configDbAccount, String configDbPassword) {
		this.dbUrl = dbUrl;
		this.dbAccount = dbAccount;
		this.dbPassword = dbPassword;
		this.configDbUrl = configDbUrl;
		this.configDbAccount = configDbAccount;
		this.configDbPassword = configDbPassword;
	}
}
