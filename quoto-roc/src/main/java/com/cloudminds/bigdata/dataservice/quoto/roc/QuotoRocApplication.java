package com.cloudminds.bigdata.dataservice.quoto.roc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Configuration;

import com.cloudminds.bigdata.dataservice.quoto.roc.config.RocSQLConfig;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;
import apijson.orm.SQLConfig;

@Configuration
@SpringBootApplication
@EnableDiscoveryClient
public class QuotoRocApplication {
	static {
		APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
			@Override
			public SQLConfig createSQLConfig() {
				return new RocSQLConfig();
			}
		};
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoRocApplication.class, args);
		APIJSONApplication.init(false);  // 4.4.0 以上需要这句来保证以上 static 代码块中给 DEFAULT_APIJSON_CREATOR 赋值会生效
	}
}
