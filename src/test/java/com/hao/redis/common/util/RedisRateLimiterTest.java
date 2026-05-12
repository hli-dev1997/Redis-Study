package com.hao.redis.common.util;

import com.hao.redis.common.interceptor.SimpleRateLimiter;
import com.hao.redis.common.monitor.RedisMetrics;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Redis 分布式限流器测试类
 *
 * 测试目的：
 * 1. 验证固定窗口算法在正常情况下的计数与限流是否准确。
 * 2. 验证时间窗口结束后，限流是否能自动重置。
 * 3. 验证高并发场景下，Lua 脚本的原子性是否得到保证。
 * 4. 验证 Redis 服务异常时，是否能成功降级到本地限流。
 *
 * 设计思路：
 * - 使用 @SpringBootTest 启动完整容器，确保 Redis 连接可用。
 * - 通过多线程模拟高并发请求，验证原子性。
 * - 使用 Mockito 手动 Mock 依赖，彻底解决 AOP 代理冲突问题。
 */
@Slf4j
@SpringBootTest
public class RedisRateLimiterTest {

    // 被测对象
    private RedisRateLimiter redisRateLimiter;

    // 真实的 RedisTemplate，用于正常流程测试
    @Autowired
    private StringRedisTemplate realRedisTemplate;

    @Value("${rate.limit.redis-fallback-ratio:0.5}")
    private double redisFallbackRatio;

    private static final String TEST_KEY = "test:api:user_info";

    @BeforeEach
    void setUp() {
        // 清理数据
        try {
            realRedisTemplate.delete("rate_limit:" + TEST_KEY);
            realRedisTemplate.delete("redis_fallback:" + TEST_KEY);
        } catch (Exception e) {
            log.warn("清理Redis Key失败|Failed_to_clean_redis_keys");
        }
    }

    @Test
    @DisplayName("正常流程测试：窗口内请求未超限")
    void testTryAcquire_Success_WithinLimit() {
        // 正常流程使用真实依赖
        redisRateLimiter = new RedisRateLimiter(realRedisTemplate, mock(SimpleRateLimiter.class), mock(RedisMetrics.class), redisFallbackRatio);
        
        int limit = 3;
        int window = 10;

        log.info("测试场景：{}秒内限流{}次|Test_scene_limit_{}_in_{}_seconds", window, limit, limit, window);

        assertTrue(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "第一次请求应成功");
        assertTrue(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "第二次请求应成功");
        assertTrue(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "第三次请求应成功");
        
        log.info("阈值已满，测试限流是否生效|Limit_reached_testing_if_limiter_is_active");
        
        assertFalse(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "第四次请求应被限流");
    }

    @Test
    @DisplayName("窗口重置测试：等待窗口过期后，限流自动恢复")
    void testTryAcquire_Reset_AfterWindowExpires() throws InterruptedException {
        redisRateLimiter = new RedisRateLimiter(realRedisTemplate, mock(SimpleRateLimiter.class), mock(RedisMetrics.class), redisFallbackRatio);
        int limit = 1;
        int window = 2;

        log.info("测试场景：窗口自动重置|Test_scene_window_auto_reset");

        assertTrue(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "窗口期内第一次请求应成功");
        assertFalse(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "窗口期内第二次请求应被限流");

        log.info("等待窗口过期...|Waiting_for_window_to_expire...");
        TimeUnit.SECONDS.sleep(window + 1);

        assertTrue(redisRateLimiter.tryAcquire(TEST_KEY, limit, window), "窗口过期后，请求应再次成功");
    }

    @Test
    @DisplayName("并发原子性测试：100个线程并发请求，验证最终成功数")
    void testTryAcquire_Concurrency_Atomicity() throws InterruptedException {
        redisRateLimiter = new RedisRateLimiter(realRedisTemplate, mock(SimpleRateLimiter.class), mock(RedisMetrics.class), redisFallbackRatio);
        int limit = 10;
        int window = 20;
        int threadCount = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        log.info("测试场景：{}个线程并发请求，验证原子性|Test_scene_{}_threads_concurrent_request_verify_atomicity", threadCount, threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (redisRateLimiter.tryAcquire(TEST_KEY, limit, window)) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        log.info("并发测试完成，成功请求数：{}，预期：{}|Concurrency_test_done,success_count={},expected={}", 
                successCount.get(), limit, successCount.get(), limit);

        assertEquals(limit, successCount.get(), "高并发下成功请求数应精确等于限流阈值，证明Lua脚本原子性");
    }

    @Test
    @DisplayName("降级容错测试：Redis异常时，验证降级逻辑被正确调用")
    void testTryAcquire_Fallback_WhenRedisIsDown() {
        // 1. 准备完全由 Mockito 控制的依赖
        StringRedisTemplate mockRedisTemplate = mock(StringRedisTemplate.class);
        SimpleRateLimiter mockFallbackLimiter = mock(SimpleRateLimiter.class);

        // 2. 使用这些 Mock 依赖来手动构造被测对象
        redisRateLimiter = new RedisRateLimiter(mockRedisTemplate, mockFallbackLimiter, mock(RedisMetrics.class), redisFallbackRatio);

        // 3. 设定 Mock 对象的行为
        // 核心修复：这里必须匹配 4 个参数 (Script, Keys, Arg1, Arg2)
        // 使用 any() 匹配每一个参数，确保 Mockito 能捕获到这次调用
        when(mockRedisTemplate.execute(any(RedisScript.class), anyList(), any(), any()))
                .thenThrow(new RedisConnectionFailureException("Mock Redis Error"));

        // 当降级方法被调用时，我们让它返回 true
        when(mockFallbackLimiter.tryAcquire(anyString(), anyDouble())).thenReturn(true);

        log.info("测试场景：Redis异常，触发本地降级|Test_scene_redis_error_trigger_local_fallback");

        int limit = 100;
        int window = 10;
        double expectedFallbackQps = (double) limit / window * redisFallbackRatio;

        // 4. 调用被测方法
        boolean result = redisRateLimiter.tryAcquire(TEST_KEY, limit, window);
        
        // 5. 断言
        assertTrue(result, "Redis异常后，应执行降级逻辑并返回其结果");

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Double> qpsCaptor = ArgumentCaptor.forClass(Double.class);
        
        // 验证降级方法是否被调用
        verify(mockFallbackLimiter, times(1)).tryAcquire(keyCaptor.capture(), qpsCaptor.capture());

        assertEquals("redis_fallback:" + TEST_KEY, keyCaptor.getValue(), "降级使用的Key应带有'redis_fallback:'前缀");
        assertEquals(expectedFallbackQps, qpsCaptor.getValue(), 0.001, "降级QPS计算不正确");
        
        log.info("降级测试通过，Redis异常时成功调用本地降级，且降级QPS计算正确|Fallback_test_passed");
    }
}
