package com.cloudminds.bigdata.dataservice.gateway.filter;
import com.cloudminds.bigdata.dataservice.gateway.utils.GatewayUtil;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 对所有的request请求的body进行解密
 * @date 2020/12/4 17:34
 * @author wei.heng
 */
@Component
public class GlobalRequestBodyDecodeFilter implements GlobalFilter, Ordered {

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		ServerHttpRequest request = exchange.getRequest();
		System.out.println("ip地址："+ GatewayUtil.getIpAddress(request)+" 访问接口："+request.getPath().toString());
		return chain.filter(exchange);
	}

	@Override
	public int getOrder() {
		return Integer.MIN_VALUE;
	}


}