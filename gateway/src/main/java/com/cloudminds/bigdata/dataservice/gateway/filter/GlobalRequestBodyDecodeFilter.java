package com.cloudminds.bigdata.dataservice.gateway.filter;
import com.cloudminds.bigdata.dataservice.gateway.utils.GatewayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 对所有的request请求的body进行解密
 * @date 2020/12/4 17:34
 * @author wei.heng
 */
@Component
public class GlobalRequestBodyDecodeFilter implements GlobalFilter, Ordered {
	Logger logger = LoggerFactory.getLogger(getClass());
	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		ServerHttpRequest request = exchange.getRequest();
		Date now = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String createTime = dateFormat.format(now);//格式化然后放入字符串
		System.out.println(createTime+"  IP地址："+ GatewayUtil.getIpAddress(request)+" 访问接口："+request.getPath().toString());
		return chain.filter(exchange);
	}

	@Override
	public int getOrder() {
		return Integer.MIN_VALUE;
	}


}