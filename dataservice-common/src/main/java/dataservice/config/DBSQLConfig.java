package dataservice.config;

import apijson.entity.DbInfo;
import apijson.framework.APIJSONSQLConfig;

import java.util.Map;

public class DBSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_CLICKHOUSE; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "harix"; // TODO 默认数据库名
	}
	

	public DBSQLConfig(Map<String, DbInfo> dbInfoMap) {
		this.dbInfoMap=dbInfoMap;
	}
}
