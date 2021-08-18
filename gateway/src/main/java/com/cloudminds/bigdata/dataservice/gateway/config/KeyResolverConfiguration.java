package com.cloudminds.bigdata.dataservice.gateway.config;

import com.cloudminds.bigdata.dataservice.gateway.utils.GatewayUtil;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class KeyResolverConfiguration {

    @Bean
    KeyResolver keyResolver() {
        return exchange -> Mono.just(GatewayUtil.getIpAndPathKey(exchange.getRequest()));
    }

    //    /**
//     * 基于路径
//     * @return
//     */
//    @Bean
//    public KeyResolver pathKeyResolver(){
//        return exchange -> Mono.just(
//                exchange.getRequest().getPath().toString());
//    }
}
