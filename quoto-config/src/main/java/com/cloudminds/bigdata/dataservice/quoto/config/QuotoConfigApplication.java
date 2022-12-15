package com.cloudminds.bigdata.dataservice.quoto.config;

import datahub.client.rest.RestEmitter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@EnableDiscoveryClient
public class QuotoConfigApplication {
    @Autowired
    private Environment environment;
    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }
    @Bean
    RestEmitter createRestEmitter() {
        return RestEmitter.create(b ->
                b.server(environment.getProperty("datahubUrl"))
                        .token(environment.getProperty("datahubToken")));
    }
    public static void main(String[] args) throws Exception {
        SpringApplication.run(QuotoConfigApplication.class, args);
    }

}
