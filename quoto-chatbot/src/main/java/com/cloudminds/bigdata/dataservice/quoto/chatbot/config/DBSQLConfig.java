package com.cloudminds.bigdata.dataservice.quoto.chatbot.config;

import apijson.framework.APIJSONSQLConfig;

public class DBSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_KYLIN; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "SV"; // TODO 默认数据库名
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
