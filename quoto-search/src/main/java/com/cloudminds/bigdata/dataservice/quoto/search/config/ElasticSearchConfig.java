package com.cloudminds.bigdata.dataservice.quoto.search.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "elasticsearch")
@Configuration
public class ElasticSearchConfig {
    private String host;
    private int port;

    public String getHost() {return host;}

    public void setHost(String host) {this.host = host;}

    public int getPort() {return port;}

    public void setPort(int port) {this.port = port;}

    // ES客户端对象
    @Bean
    public RestHighLevelClient client(){
        HttpHost host = new HttpHost(getHost(),getPort(),"http");
        return new RestHighLevelClient(RestClient.builder(host));
    }
}
