package com.cloudminds.bigdata.dataservice.quoto.search;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;


@SpringBootApplication
@EnableDiscoveryClient
public class QuotoSearchApplication {
	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoSearchApplication.class, args);
	}
}
