package com.cloudminds.bigdata.dataservice.quoto.chatbot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Configuration;

import com.cloudminds.bigdata.dataservice.quoto.chatbot.config.ChatbotSQLConfig;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;

@Configuration
@SpringBootApplication
@EnableDiscoveryClient
public class QuotoChatbotApplication {
	static {
		APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
			@Override
			public ChatbotSQLConfig createSQLConfig() {
				return new ChatbotSQLConfig();
			}
		};
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoChatbotApplication.class, args);
		APIJSONApplication.init(false);  // 4.4.0 以上需要这句来保证以上 static 代码块中给 DEFAULT_APIJSON_CREATOR 赋值会生效
	}
}
