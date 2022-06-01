package com.cloudminds.bigdata.dataservice.label.manage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class LabelManageApplication {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(LabelManageApplication.class, args);
    }
}
