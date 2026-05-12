package com.hao.redis.common.aspect;

import com.hao.redis.common.util.RedisSlotUtil;
import com.hao.redis.integration.cluster.RedisClusterTopologyCache;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Redis 路由监控切面
 *
 * 类职责：
 * 监控 Redis 写操作，并打印 Key 的路由信息（Key -> Slot -> Node）。
 *
 * 设计目的：
 * - 提供实时的数据分布可观测性。
 * - 帮助开发者理解 Key 的分片规则，排查热点 Key 问题。
 *
 * 核心实现思路：
 * - 使用 AOP 拦截 `RedisClient` 的写方法。
 * - 在方法执行后，获取第一个参数（通常是 Key）。
 * - 调用 `RedisSlotUtil` 计算 Slot。
 * - 调用 `RedisClusterTopologyCache` 查询 Node。
 * - 打印日志。
 */
@Slf4j
@Aspect
@Component
public class RedisRouteMonitorAspect {

    @Autowired
    private RedisClusterTopologyCache topologyCache;

    /**
     * 定义切点：拦截 RedisClient 中所有公共的、以 "set" 或 "hset" 等写操作开头的方法
     */
    @Pointcut("execution(public * com.hao.redis.integration.redis.RedisClient.set*(..)) || " +
              "execution(public * com.hao.redis.integration.redis.RedisClient.hset*(..)) || " +
              "execution(public * com.hao.redis.integration.redis.RedisClient.lpush*(..)) || " +
              "execution(public * com.hao.redis.integration.redis.RedisClient.rpush*(..)) || " +
              "execution(public * com.hao.redis.integration.redis.RedisClient.sadd*(..)) || " +
              "execution(public * com.hao.redis.integration.redis.RedisClient.zadd*(..))")
    public void redisWriteMethods() {
    }

    /**
     * 后置通知：在写操作方法执行后打印路由信息
     */
    @After("redisWriteMethods()")
    public void logRouteInfo(JoinPoint joinPoint) {
        try {
            Object[] args = joinPoint.getArgs();
            if (args == null || args.length == 0 || !(args[0] instanceof String)) {
                return; // 无法获取 Key，不处理
            }

            String key = (String) args[0];

            // 1. 计算 Slot
            int slot = RedisSlotUtil.getSlot(key);

            // 2. 查询 Node
            String node = topologyCache.getNodeBySlot(slot);

            // 3. 打印日志
//            log.info("Redis路由监控|Redis_route_monitor, key={}, slot={}, node={}", key, slot, node);

        } catch (Exception e) {
            log.warn("Redis路由监控异常|Redis_route_monitor_error", e);
        }
    }
}
