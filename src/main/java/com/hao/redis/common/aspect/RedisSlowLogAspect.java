package com.hao.redis.common.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Redis 慢操作日志切面
 * <p>
 * 类职责：
 * 自动拦截 RedisClient 的所有方法调用，统计执行时间，对超过阈值的操作打印告警日志。
 * <p>
 * 设计目的：
 * 1. 快速定位由于网络延迟或大规模数据点导致的操作变慢。
 * 2. 识别不合理的 Redis 使用模式（如全量读取 Key）。
 * <p>
 * 【性能调优 - 重点说明】
 * 1. 零侵入采集：基于 Spring AOP 机制，在不改动业务代码的前提下，实现了对万级并发链路的耗时“探针”。
 * 2. 性能诊断：通过记录慢操作的 method 和 args，能精准识别出哪些大 Key 或复杂命令导致了 Redis 服务端的毛刺现象，是解决长尾延迟的关键。
 *
 * 阈值设定：100ms
 */
@Aspect
@Component
@Slf4j
public class RedisSlowLogAspect {

    // 慢操作阈值（毫秒）
    private static final long SLOW_THRESHOLD_MS = 100;

    /**
     * 环绕增强：统计 Redis 客户端方法耗时
     * 
     * 切点表达式：拦截 com.hao.redis.integration.redis 下所有实现类的方法
     */
    @Around("execution(* com.hao.redis.integration.redis.RedisClientImpl.*(..))")
    public Object logSlowOperation(ProceedingJoinPoint pjp) throws Throwable {
        long start = System.currentTimeMillis();
        
        try {
            // 执行目标方法
            return pjp.proceed();
        } finally {
            long cost = System.currentTimeMillis() - start;
            
            // 超过阈值则打印告警
            if (cost > SLOW_THRESHOLD_MS) {
                log.warn("🚨 Redis慢操作告警|Redis_slow_op_detected, method={}, cost={}ms, args={}",
                        pjp.getSignature().toShortString(),
                        cost,
                        pjp.getArgs());
            } else if (log.isDebugEnabled()) {
                log.debug("Redis操作耗时|Redis_op_cost, method={}, cost={}ms",
                        pjp.getSignature().toShortString(),
                        cost);
            }
        }
    }
}
