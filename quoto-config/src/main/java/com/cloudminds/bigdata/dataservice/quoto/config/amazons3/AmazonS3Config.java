package com.cloudminds.bigdata.dataservice.quoto.config.amazons3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AmazonS3Config {
    @Value("${accessKey}")
    private String accessKey;
    @Value("${secretKey}")
    private String secretKey;
    @Value("${endpoint}")
    private String endpoint;

    @Bean(name = "amazonS3Client")
    public AmazonS3 amazonS3Client() {
        //设置连接时的参数
        ClientConfiguration config = new ClientConfiguration();
        //设置连接方式为HTTP，可选参数为HTTP和HTTPS
        config.setProtocol(Protocol.HTTP);
        //设置网络访问超时时间
        config.setConnectionTimeout(5000);
        config.setUseExpectContinue(true);
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        //设置Endpoint
        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "default"))
                .withPathStyleAccessEnabled(true).build();
    }
}
