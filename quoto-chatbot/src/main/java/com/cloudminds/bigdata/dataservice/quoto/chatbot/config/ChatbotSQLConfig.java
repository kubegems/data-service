package com.cloudminds.bigdata.dataservice.quoto.chatbot.config;

import apijson.framework.APIJSONSQLConfig;

public class ChatbotSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_KYLIN; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "SV"; // TODO 默认数据库名
	}

	// 数据库配置 dataService
	@Override
	public String getDBUri() {
		if (isKYLIN()) {
			return  "jdbc:kylin://172.16.23.134:7070/SV";
		}
		if (isDATASERVICE()) {
			return "jdbc:mysql://bigdata-mysql:3306"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBAccount() {
		if (isKYLIN()) {			
			return "ADMIN";
		}
		if (isDATASERVICE()) {
			return "root"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBPassword() {
		if (isKYLIN()) {			
			return "KYLIN";
		}
		if (isDATASERVICE()) {
			return "bigdata-mysql-cloud1688"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBVersion() {
		return "5.7.24";
	}
}
