package com.cloudminds.bigdata.dataservice.quoto.roc;

import com.purgeteam.dynamic.config.starter.annotation.EnableDynamicConfigEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Configuration;

@Configuration
@SpringBootApplication
@EnableDiscoveryClient
@EnableDynamicConfigEvent
public class QuotoRocApplication {
	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoRocApplication.class, args);
	}
}
