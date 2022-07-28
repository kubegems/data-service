package com.cloudminds.bigdata.dataservice.quoto.chatbot;

import com.purgeteam.dynamic.config.starter.annotation.EnableDynamicConfigEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

@Configuration
@SpringBootApplication
@EnableDiscoveryClient
@EnableDynamicConfigEvent
@RefreshScope
public class QuotoChatbotApplication {
	public static void main(String[] args) {
		SpringApplication.run(QuotoChatbotApplication.class, args);
	}
}
