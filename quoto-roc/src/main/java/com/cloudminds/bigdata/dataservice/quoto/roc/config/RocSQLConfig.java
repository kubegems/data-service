package com.cloudminds.bigdata.dataservice.quoto.roc.config;

import java.util.HashMap;
import java.util.Map;

import apijson.framework.APIJSONSQLConfig;

public class RocSQLConfig extends APIJSONSQLConfig {
	static {
		DEFAULT_DATABASE = DATABASE_CLICKHOUSE; // TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "harix"; // TODO 默认模式名，改成你自己的，默认情况是 MySQL: sys, PostgreSQL: public, SQL Server: dbo,
									// Oracle:
		TABLE_KEY_MAP.put("RobotQuoto", "mv_roc_digital_all");
		// 新增表 mv_roc_digital_all 表字段提供给接口的别名
		Map<String, String> value = new HashMap<String, String>();
		value.put("\"tenantId\"", "\"tenant_id\"");
		value.put("\"robotId\"", "\"robot_id\"");
		value.put("\"robotType\"", "\"rebot_type\"");
		value.put("\"userId\"", "\"user_id\"");
		value.put("\"rodType\"", "\"rod_type\"");
		value.put("\"time\"", "\"log_time\"");
		tableColumnMap.put("mv_roc_digital_all" + "_processColumn", value);
		value = new HashMap<String, String>();
		// 和上面是一样的值
		value.put("\"tenantId\"", "\"tenant_id\"");
		value.put("\"robotId\"", "\"robot_id\"");
		value.put("\"robotType\"", "\"rebot_type\"");
		value.put("\"userId\"", "\"user_id\"");
		value.put("\"rodType\"", "\"rod_type\"");
		value.put("\"time\"", "\"log_time\"");
		// 指标函数
		value.put("\"robotOnlineTick\"", "round(countIf(rod_type = 'robotOnlineTick')/6, 1)");
		value.put("\"robotConnect\"", "countIf(rod_type = 'robotConnect')");
		value.put("\"robotAbnormalDisconnect\"", "countIf(rod_type = 'robotAbnormalDisconnect')");
		value.put("\"robotAlarm\"", "countIf(rod_type = 'robotAlarm')");
		value.put("\"FRCounter\"", "countIf(rod_type = 'FRCounter')");
		value.put("\"voiceClipCounter\"", "countIf(rod_type = 'voiceClipCounter')");
		value.put("\"asrCounter\"", "countIf(rod_type = 'asrCounter')");
		value.put("\"avStreamSize\"", "sumIf(size, rod_type = 'avStream')");
		value.put("\"avStreamDuration\"", "sumIf(duration, rod_type = 'avStream')");
		value.put("\"hiInventionCounter\"", "countIf(rod_type = 'hiInventionCounter')");
		value.put("\"aiResponseCounter\"", "countIf(rod_type = 'aiResponseCounter')");
		value.put("\"hiServiceLogin\"", "countIf(rod_type = 'hiServiceLogin')");
		value.put("\"hiInventionCounter\"", "countIf(rod_type = 'hiServiceCounter')");
		value.put("\"dateTime\"", "toDate(log_time)");
		tableColumnMap.put("mv_roc_digital_all" + "_aliasColumn", value);
		value = new HashMap<String, String>();
		value.put("tenantId", "tenant_id");
		value.put("robotId", "robotId");
		value.put("robotType", "robotType");
		value.put("userId", "userId");
		value.put("rodType", "rodType");
		value.put("time", "log_time");
		tableColumnMap.put("mv_roc_digital_all" + "_realColumn", value);
	}

	// 数据库配置

	@Override
	public String getDBUri() {
		return "jdbc:clickhouse://172.16.31.116:8123";
	}

	@Override
	public String getDBAccount() {
		return "readonly";
	}

	@Override
	public String getDBPassword() {
		return "123456";
	}

	@Override
	public String getDBVersion() {
		return "5.7.24";
	}
}
