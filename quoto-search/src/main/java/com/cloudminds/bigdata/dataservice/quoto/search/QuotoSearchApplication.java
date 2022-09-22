package com.cloudminds.bigdata.dataservice.quoto.search;

import com.cloudminds.bigdata.dataservice.quoto.search.config.HttpComponentsClientRestfulHttpRequestFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;


@SpringBootApplication
@EnableDiscoveryClient
public class QuotoSearchApplication {
	@Bean
	RestTemplate restTemplate() {
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setRequestFactory(new HttpComponentsClientRestfulHttpRequestFactory());
		return restTemplate;
	}
	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoSearchApplication.class, args);
	}
}
