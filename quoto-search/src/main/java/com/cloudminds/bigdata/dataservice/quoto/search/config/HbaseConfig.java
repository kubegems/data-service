package com.cloudminds.bigdata.dataservice.quoto.search.config;

import lombok.Data;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
@Data
@ConfigurationProperties(prefix = "hbase")
@Configuration
public class HbaseConfig {
    private String quorum;
    //获取数据库连接
    @Bean
    public Connection hbaseConnect(){
        Connection connection = null;
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM,getQuorum());
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
