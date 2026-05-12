package com.hao.redis.config;


import com.hao.redis.common.interceptor.VisitInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web 拦截器配置类
 *
 * 类职责：
 * 统一注册系统内的拦截器并配置拦截路径规则。
 *
 * 设计目的：
 * 1. 集中管理拦截器装配，避免分散配置导致的遗漏。
 * 2. 规范访问统计与安全控制的统一入口。
 *
 * 为什么需要该类：
 * 拦截器需要在 WebMvcConfigurer 中显式注册才能生效，缺少统一配置会导致逻辑失效。
 *
 * 核心实现思路：
 * - 使用 InterceptorRegistry 注册访问拦截器。
 * - 对系统统计接口进行放行，避免拦截器自触发。
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    @Autowired
    private VisitInterceptor visitInterceptor;

    /**
     * 注册拦截器链
     *
     * 实现逻辑：
     * 1. 注册访问拦截器。
     * 2. 配置全路径拦截并放行指定统计接口。
     *
     * @param registry 拦截器注册器
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 实现思路：
        // 1. 默认拦截全部请求。
        // 2. 放行统计接口，避免误拦截。
        registry.addInterceptor(visitInterceptor)
                .addPathPatterns("/**")
                .excludePathPatterns(
                        "/system/uv",
                        "/weibo/system/uv",
                        // 选股信号接口压测时 QPS 极高，每次 sadd 会产生大量 Redis 慢日志
                        // 量化选股场景无需 UV 统计，直接排除
                        "/stock/**"
                );
    }
}
