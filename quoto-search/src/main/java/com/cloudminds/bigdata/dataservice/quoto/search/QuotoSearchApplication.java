package com.cloudminds.bigdata.dataservice.quoto.search;

import com.cloudera.io.netty.channel.ConnectTimeoutException;
import com.cloudminds.bigdata.dataservice.quoto.search.config.HttpComponentsClientRestfulHttpRequestFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


@SpringBootApplication
@EnableDiscoveryClient
public class QuotoSearchApplication {
	private static final int CONNECT_TIMEOUT = 5000;
	private static final int CONNECTION_MANAGER_CONNECTION_REQUEST_TIMEOUT = 0;
	private static final int SOCKET_TIMEOUT = 5000;
	private static final int MAX_TOTAL = 1000;
	private static final int MAX_PER_ROUTE = 32;

	@Bean
	public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setMaxTotal(MAX_TOTAL);
		connectionManager.setDefaultMaxPerRoute(MAX_PER_ROUTE);
		connectionManager.setValidateAfterInactivity(CONNECT_TIMEOUT);
		return connectionManager;
	}
	@Bean
	public RequestConfig requestConfig() {
		RequestConfig result = RequestConfig.custom()
				.setConnectionRequestTimeout(CONNECTION_MANAGER_CONNECTION_REQUEST_TIMEOUT)
				.setConnectTimeout(CONNECT_TIMEOUT)
				.setSocketTimeout(SOCKET_TIMEOUT)
				.build();

		return result;
	}
	public static class CustomRequestRetryHandler extends DefaultHttpRequestRetryHandler {
		protected static final Collection<Class<? extends IOException>> ignoredExceptions =
				Arrays.asList(ConnectTimeoutException.class, UnknownHostException.class);

		public CustomRequestRetryHandler(final int retryCount) {
			super(retryCount, true, ignoredExceptions);
		}
	}
	@Bean
	public CloseableHttpClient httpClient(PoolingHttpClientConnectionManager poolingHttpClientConnectionManager, RequestConfig requestConfig) {
		CloseableHttpClient result = HttpClientBuilder
				.create()
				.setConnectionManager(poolingHttpClientConnectionManager)
				.setDefaultRequestConfig(requestConfig)
				.setRetryHandler(new CustomRequestRetryHandler(5))
				.build();

		return result;
	}
	@Bean
	RestTemplate restTemplate() {
		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		factory.setHttpClient(httpClient(poolingHttpClientConnectionManager(), requestConfig()));
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setRequestFactory(factory);
		return restTemplate;
	}
	public static void main(String[] args) throws Exception {
		SpringApplication.run(QuotoSearchApplication.class, args);
	}
}
