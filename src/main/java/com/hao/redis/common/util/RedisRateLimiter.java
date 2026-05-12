package com.hao.redis.common.util;

import com.hao.redis.common.interceptor.SimpleRateLimiter;
import com.hao.redis.common.monitor.RedisMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Redis分布式限流工具类
 *
 * 类职责：
 * 提供基于Redis + Lua脚本的分布式限流能力，确保高并发场景下的系统稳定性。
 *
 * 设计目的：
 * 1. 保证操作原子性：使用Lua脚本封装“INCR+EXPIRE+校验”逻辑，避免并发竞态。
 * 2. 降低网络开销：将多次Redis交互合并为一次网络请求。
 * 3. 提供容错机制：当Redis不可用时，自动降级为本地保守限流，避免异常时完全放行。
 *
 * 为什么需要该类：
 * 分布式限流必须具备原子性与高可用特性，独立封装可降低业务侵入。
 *
 * 实现思路：
 * - 采用计数器固定窗口算法。
 * - 通过 Lua 脚本在 Redis 端原子执行计数与过期时间设置。
 * - 捕获 Redis 执行异常，默认执行本地保守限流降级策略。
 */
@Slf4j
@Component
public class RedisRateLimiter {

    private final StringRedisTemplate stringRedisTemplate;
    private final DefaultRedisScript<Long> limitScript;
    private final SimpleRateLimiter fallbackRateLimiter;
    private final double redisFallbackRatio;
    private final RedisMetrics redisMetrics;

    // Lua 脚本：固定窗口计数器
    // KEYS[1]: 限流键
    // ARGV[1]: 限流阈值
    // ARGV[2]: 时间窗口(秒)
    // 优化：增加 TTL 兜底校验，防止 Key 变为永不过期的僵尸 Key
    private static final String LUA_SCRIPT_TEXT =
            "local key = KEYS[1] " +
            "local limit = tonumber(ARGV[1]) " +
            "local window = tonumber(ARGV[2]) " +
            "local current = redis.call('INCR', key) " +
            "if current == 1 or redis.call('TTL', key) == -1 then " +
            "    redis.call('EXPIRE', key, window) " +
            "end " +
            "if current > limit then " +
            "    return 0 " +
            "end " +
            "return 1";

    /**
     * Redis 限流器构造方法
     *
     * 实现逻辑：
     * 1. 注入 StringRedisTemplate。
     * 2. 初始化 Lua 脚本并设置返回类型。
     *
     * @param stringRedisTemplate Redis 模板
     */
    public RedisRateLimiter(StringRedisTemplate stringRedisTemplate,
                            SimpleRateLimiter fallbackRateLimiter,
                            RedisMetrics redisMetrics,
                            @Value("${rate.limit.redis-fallback-ratio:0.5}") double redisFallbackRatio) {
        // 实现思路：
        // 1. 注入模板并完成脚本初始化。
        this.stringRedisTemplate = stringRedisTemplate;
        this.fallbackRateLimiter = fallbackRateLimiter;
        this.redisMetrics = redisMetrics;
        this.redisFallbackRatio = normalizeFallbackRatio(redisFallbackRatio);
        // 初始化 Lua 脚本
        this.limitScript = new DefaultRedisScript<>();
        this.limitScript.setScriptText(LUA_SCRIPT_TEXT);
        this.limitScript.setResultType(Long.class);
    }

    /**
     * 尝试获取访问许可
     *
     * 实现逻辑：
     * 1. 构造 Redis 键。
     * 2. 执行 Lua 脚本进行原子计数校验。
     * 3. 处理 Redis 异常，执行降级策略。
     *
     * @param key 业务键（如用户ID、IP）
     * @param limit 限制次数
     * @param windowSeconds 时间窗口（秒）
     * @return true表示允许访问，false表示拒绝访问
     */
    public boolean tryAcquire(String key, int limit, int windowSeconds) {
        // 实现思路：
        // 1. 构造 Redis 键。
        // 2. 执行 Lua 脚本进行原子计数校验。
        // 3. 处理 Redis 异常，执行降级策略。

        // 构造完整的 Redis 键，增加前缀避免冲突
        String redisKey = "rate_limit:" + key;

        try {
            // 核心代码：执行 Lua 脚本，原子性判断是否限流
            // 参数说明：KEYS=[限流键], ARGV=[阈值, 窗口秒数]
            Long result = stringRedisTemplate.execute(
                    limitScript,
                    Collections.singletonList(redisKey),
                    String.valueOf(limit),
                    String.valueOf(windowSeconds)
            );

            // Lua脚本返回1表示允许，0表示拒绝
            boolean allowed = result != null && result == 1L;
            
            // 埋点监控
            if (allowed) {
                redisMetrics.recordRateLimitHit();
            } else {
                redisMetrics.recordRateLimitBlock();
            }
            
            return allowed;

        } catch (Exception e) {
            // 实现思路：
            // 1. 捕获 Redis 连接超时、读写失败等异常。
            // 2. 记录错误日志，保留现场信息。
            // 3. 降级到本地保守限流，避免异常时完全放行。
            log.error("Redis限流服务异常_触发本地降级|Redis_limiter_error_fallback_to_local,key={},error={}", key, e.getMessage(), e);
            // 核心修复：根据 limit 和 window 计算正确的 QPS
            double fallbackQps = calculateFallbackQps(limit, windowSeconds);
            boolean allowed = fallbackRateLimiter.tryAcquire(buildFallbackKey(key), fallbackQps);
            if (!allowed) {
                log.warn("Redis限流异常_本地降级拦截|Redis_limiter_error_local_block,key={},fallbackQps={}",
                        key, formatQps(fallbackQps));
            }
            return allowed;
        }
    }

    /**
     * 计算降级QPS
     *
     * 实现逻辑：
     * 1. 按比例下调限流阈值。
     * 2. 兜底为正数，避免非法速率。
     *
     * @param limit 原始阈值
     * @param windowSeconds 时间窗口
     * @return 降级QPS
     */
    private double calculateFallbackQps(int limit, int windowSeconds) {
        // 实现思路：
        // 1. 按比例计算保守阈值。
        // 核心修复：QPS = 次数 / 时间
        double originalQps = (windowSeconds > 0) ? (double) limit / windowSeconds : limit;
        double fallbackQps = originalQps * redisFallbackRatio;
        return fallbackQps > 0 ? fallbackQps : 0.1D;
    }

    /**
     * 构造降级限流键
     *
     * 实现逻辑：
     * 1. 使用固定前缀区分降级限流器。
     *
     * @param key 业务键
     * @return 降级限流键
     */
    private String buildFallbackKey(String key) {
        // 实现思路：
        // 1. 前缀隔离降级限流键。
        return "redis_fallback:" + key;
    }

    /**
     * 规整降级比例
     *
     * 实现逻辑：
     * 1. 保证比例在(0,1]范围内。
     *
     * @param ratio 配置比例
     * @return 规整后的比例
     */
    private double normalizeFallbackRatio(double ratio) {
        // 实现思路：
        // 1. 兜底为0.5，避免非法配置。
        if (ratio > 0 && ratio <= 1) {
            return ratio;
        }
        return 0.5D;
    }

    /**
     * 格式化QPS
     *
     * 实现逻辑：
     * 1. 保留两位小数，便于日志观察。
     *
     * @param qps QPS数值
     * @return 格式化字符串
     */
    private String formatQps(double qps) {
        // 实现思路：
        // 1. 使用两位小数输出。
        return String.format("%.2f", qps);
    }
}
