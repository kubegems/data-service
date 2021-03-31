package com.cloudminds.bigdata.dataservice.standard.manage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;


@SpringBootApplication
@EnableDiscoveryClient
public class StandardManageApplication {
	public static void main(String[] args) throws Exception {
		SpringApplication.run(StandardManageApplication.class, args);
	}
}
