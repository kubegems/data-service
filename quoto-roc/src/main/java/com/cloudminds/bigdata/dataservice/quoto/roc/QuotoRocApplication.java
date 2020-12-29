package com.cloudminds.bigdata.dataservice.quoto.roc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class QuotoRocApplication 
{
    public static void main( String[] args )
    {
    	 SpringApplication.run(QuotoRocApplication.class, args);
    }
}
