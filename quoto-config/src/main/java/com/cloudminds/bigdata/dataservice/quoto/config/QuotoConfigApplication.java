package com.cloudminds.bigdata.dataservice.quoto.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;


@SpringBootApplication
@EnableDiscoveryClient
public class QuotoConfigApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoConfigApplication.class, args);
	}
}
