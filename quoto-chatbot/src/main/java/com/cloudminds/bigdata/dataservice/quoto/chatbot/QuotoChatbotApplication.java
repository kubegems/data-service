package com.cloudminds.bigdata.dataservice.quoto.chatbot;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import com.cloudminds.bigdata.dataservice.quoto.chatbot.config.DBSQLConfig;
import com.purgeteam.dynamic.config.starter.annotation.EnableDynamicConfigEvent;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;
import apijson.orm.SQLConfig;

@Configuration
@SpringBootApplication
@EnableDiscoveryClient
@EnableDynamicConfigEvent
@RefreshScope
public class QuotoChatbotApplication {
	public static Map<String, String> dbInfo=new HashMap<String, String>();

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(QuotoChatbotApplication.class).run(args);
		dbInfo.put("dbUrl", context.getEnvironment().getProperty("dbUrl"));
		dbInfo.put("dbAccount", context.getEnvironment().getProperty("dbAccount"));
		dbInfo.put("dbPassword", context.getEnvironment().getProperty("dbPassword"));
		dbInfo.put("configDbUrl", context.getEnvironment().getProperty("configDbUrl"));
		dbInfo.put("configDbAccount", context.getEnvironment().getProperty("configDbAccount"));
		dbInfo.put("configDbPassword", context.getEnvironment().getProperty("configDbPassword"));
		APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
			@Override
			public SQLConfig createSQLConfig() {
				return new DBSQLConfig(dbInfo.get("dbUrl"), dbInfo.get("dbAccount"),dbInfo.get("dbPassword"), dbInfo.get("configDbUrl"),dbInfo.get("configDbAccount"), dbInfo.get("configDbPassword"));
			}
		};
		APIJSONApplication.init(false);  // 4.4.0 以上需要这句来保证以上 static 代码块中给 DEFAULT_APIJSON_CREATOR 赋值会生效
	}
}
