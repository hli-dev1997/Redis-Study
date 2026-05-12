package com.hao.redis.common.util;

import com.hao.redis.common.model.RedisLogicalData;
import com.hao.redis.common.monitor.RedisMetrics;
import com.hao.redis.integration.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * 缓存击穿防护工具类（逻辑过期版）
 * <p>
 * 类职责：
 * 提供基于“逻辑过期”机制的缓存查询模板方法，解决热点 Key 击穿问题。
 * <p>
 * 核心算法：
 * 1. 查询缓存，若未命中则返回空（需配合预热）。
 * 2. 若命中，判断逻辑时间是否过期。
 * 3. 未过期 -> 直接返回。
 * 4. 已过期 -> 尝试获取互斥锁。
 *    - 获锁成功 -> 开启异步线程查库重建 -> 返回旧值。
 *    - 获锁失败 -> 直接返回旧值。
 */
@Slf4j
@Component
public class CacheBreakdownUtil {

    @Autowired
    private RedisClient<String> redisClient;

    @Autowired
    private RedisMetrics redisMetrics;

    // 注入 IO 密集型线程池（用于异步查库）
    // 修正：使用 ThreadPoolConfig 中定义的 Bean 名称 "ioTaskExecutor"
    // 且类型应为 Executor 或 ThreadPoolTaskExecutor
    @Autowired
    @Qualifier("ioTaskExecutor")
    private Executor executorService;

    // 锁的前缀
    private static final String LOCK_PREFIX = "lock:logical:";

    /**
     * 逻辑过期查询模板方法
     *
     * @param keyPrefix Redis Key 前缀
     * @param id 业务 ID
     * @param type 业务数据类型 Class
     * @param dbFallback 数据库查询函数（函数式接口）
     * @param expireSeconds 逻辑过期时间（秒）
     * @param <R> 返回值类型
     * @param <ID> ID 类型
     * @return 业务数据
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            long expireSeconds
    ) {
        String key = keyPrefix + id;

        // 1. 查询 Redis
        String json = redisClient.get(key);

        // 2. 如果缓存未命中，说明不是热点 Key 或者未预热
        // 逻辑过期方案的前提是：热点 Key 必须提前预热（永不过期）
        // 这里可以选择直接返回 null，或者降级为普通查库（互斥锁方案）
        // 为了演示纯粹的逻辑过期，这里假设未命中直接返回 null（或由调用方处理）
        if (json == null || json.isBlank()) {
            log.warn("逻辑过期缓存未命中_请检查预热|Logical_cache_miss,key={}", key);
            return null;
        }

        // 3. 反序列化为逻辑封装对象
        // 注意：这里需要处理泛型反序列化，简单起见假设 JsonUtil 支持
        // 实际工程中建议使用 TypeReference
        RedisLogicalData<R> logicalData = JsonUtil.toBean(json, RedisLogicalData.class, type);
        
        if (logicalData == null) {
            return null;
        }

        R data = logicalData.getData();
        LocalDateTime expireTime = logicalData.getExpireTime();

        // 4. 判断是否逻辑过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，直接返回
            return data;
        }

        // 5. 已过期，尝试异步重建
        String lockKey = LOCK_PREFIX + key;
        
        // 尝试获取互斥锁（非阻塞）
        // 这里的 tryLock 需要在 RedisClient 中实现，或者使用 setnx
        boolean isLock = tryLock(lockKey);

        if (isLock) {
            log.info("逻辑过期_获取锁成功_启动异步重建|Logical_expire_async_rebuild,key={}", key);
            // 埋点监控：记录一次缓存重建
            redisMetrics.recordCacheRebuild();
            
            // 6. 开启独立线程重建缓存
            executorService.execute(() -> {
                try {
                    // 查数据库
                    R newR = dbFallback.apply(id);
                    // 重建缓存（重置逻辑过期时间）
                    this.saveLogicalData(key, newR, expireSeconds);
                    log.info("异步重建完成|Async_rebuild_done,key={}", key);
                } catch (Exception e) {
                    log.error("异步重建失败|Async_rebuild_fail,key={}", key, e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        } else {
            log.info("逻辑过期_获取锁失败_返回旧值|Logical_expire_lock_fail_return_old,key={}", key);
        }

        // 7. 无论是否获锁，都返回旧值（高可用）
        return data;
    }

    /**
     * 写入逻辑过期数据（预热/重建用）
     */
    public <R> void saveLogicalData(String key, R data, long expireSeconds) {
        // 封装数据
        RedisLogicalData<R> logicalData = new RedisLogicalData<>(expireSeconds, data);
        // 写入 Redis（不设置 TTL，即永不过期）
        redisClient.set(key, JsonUtil.toJson(logicalData));
    }

    // --- 简易分布式锁辅助方法 ---

    private boolean tryLock(String key) {
        // SETNX key "1"
        Boolean success = redisClient.setnx(key, "1");
        // 设置锁的兜底过期时间，防止死锁（如 10 秒）
        if (Boolean.TRUE.equals(success)) {
            redisClient.expire(key, 10);
            return true;
        }
        return false;
    }

    private void unlock(String key) {
        redisClient.del(key);
    }
}
