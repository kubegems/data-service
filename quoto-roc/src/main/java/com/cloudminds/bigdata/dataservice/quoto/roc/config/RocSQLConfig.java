package com.cloudminds.bigdata.dataservice.quoto.roc.config;

import apijson.framework.APIJSONSQLConfig;

public class RocSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_CLICKHOUSE; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "harix"; // TODO 默认数据库名
	}

	// 数据库配置 dataService
	@Override
	public String getDBUri() {
		if (isCLICKHOUSE()) {
			return "jdbc:clickhouse://172.16.31.116:8123"; // TODO 改成你自己的
		}
		if (isDATASERVICE()) {
			return "jdbc:mysql://10.12.32.229:3306"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBAccount() {
		if (isCLICKHOUSE()) {
			return "readonly"; // TODO 改成你自己的
		}
		if (isDATASERVICE()) {
			return "root"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBPassword() {
		if (isCLICKHOUSE()) {
			return "123456"; // TODO 改成你自己的
		}
		if (isDATASERVICE()) {
			return "root"; // TODO 改成你自己的
		}
		return null;
	}

	@Override
	public String getDBVersion() {
		return "5.7.24";
	}
}
