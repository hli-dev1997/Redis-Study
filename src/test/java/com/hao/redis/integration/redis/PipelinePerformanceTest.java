package com.hao.redis.integration.redis;

import com.hao.redis.dal.model.WeiboPost;
import com.hao.redis.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis Pipeline 性能优化测试类
 * <p>
 * 测试目的：
 * 1. 验证 Pipeline 批量操作的功能正确性。
 * 2. 对比循环调用 (Loop) 与 流水线调用 (Pipeline) 的性能差异。
 * 3. 证明在高并发/大延迟环境下，Pipeline 对系统吞吐量的巨大提升。
 */
@Slf4j
@SpringBootTest
public class PipelinePerformanceTest {

    @Autowired
    private RedisClient<String> redisClient;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String TEST_HASH_KEY = "test:pipeline:hash";
    private static final int DATA_COUNT = 50; // 测试 50 条数据

    @BeforeEach
    public void setup() {
        log.info("测试环境准备：写入 {} 条基准数据", DATA_COUNT);
        redisTemplate.delete(TEST_HASH_KEY);
        for (int i = 0; i < DATA_COUNT; i++) {
            String field = "f_" + i;
            WeiboPost post = new WeiboPost();
            post.setPostId(field);
            post.setContent("Test Content " + i);
            redisClient.hset(TEST_HASH_KEY, field, JsonUtil.toJson(post));
        }
    }

    @AfterEach
    public void tearDown() {
        redisTemplate.delete(TEST_HASH_KEY);
    }

    @Test
    @DisplayName("Pipeline 性能对比测试：Loop vs HMGET vs Pipeline")
    public void testPipelinePerformanceMetrics() {
        List<String> fieldList = new ArrayList<>();
        for (int i = 0; i < DATA_COUNT; i++) {
            fieldList.add("f_" + i);
        }

        log.info("--- 开始性能对比测试 (数据量: {}) ---", DATA_COUNT);

        // 1. 循环调用 (最差方案)
        long start1 = System.currentTimeMillis();
        List<String> result1 = new ArrayList<>();
        for (String field : fieldList) {
            result1.add(redisClient.hget(TEST_HASH_KEY, field));
        }
        long cost1 = System.currentTimeMillis() - start1;
        log.info("方案 A [循环调用 HGET] 耗时: {}ms", cost1);

        // 2. 原生 HMGET (已有批量能力，但不能跨 Key 或执行不同指令组合)
        long start2 = System.currentTimeMillis();
        List<String> result2 = redisClient.hmget(TEST_HASH_KEY, fieldList);
        long cost2 = System.currentTimeMillis() - start2;
        log.info("方案 B [原生 HMGET] 耗时: {}ms", cost2);

        // 3. Pipeline 优化方案
        long start3 = System.currentTimeMillis();
        List<String> result3 = redisClient.hmgetPipelined(TEST_HASH_KEY, fieldList);
        long cost3 = System.currentTimeMillis() - start3;
        log.info("方案 C [Pipeline 批量获取] 耗时: {}ms", cost3);

        // 结果校验
        assertEquals(DATA_COUNT, result1.size());
        assertEquals(result1, result2, "HMGET 结果应与循环调用一致");
        assertEquals(result1, result3, "Pipeline 结果应与循环调用一致");

        // 性能提升证明
        double improvement = (double) cost1 / cost3;
        log.info("性能提升测试结论：Pipeline 相比循环调用提升了 {:.2f} 倍", improvement);
        
        // 通常在本地测试中，Pipeline 也会比循环快 5 倍以上（不计网络真实延迟）
        assertTrue(cost3 <= cost1, "Pipeline 性能理应不低于循环调用");
    }

    @Test
    @DisplayName("Pipeline 功能测试：通用执行能力")
    public void testGenericPipeline() {
        String key1 = "pipe:k1";
        String key2 = "pipe:k2";
        
        List<Object> results = redisClient.executePipelined(connection -> {
            org.springframework.data.redis.connection.StringRedisConnection stringConn = 
                    (org.springframework.data.redis.connection.StringRedisConnection) connection;
            // 在同一个流水线中执行不同类型的命令
            stringConn.set(key1, "v1");
            stringConn.incr(key2);
            stringConn.get(key1);
            return null;
        });

        log.info("Pipeline 执行结果集: {}", results);
        
        // 验证 Pipeline 返回了多条执行结果
        assertNotNull(results);
        assertTrue(results.size() >= 3);
        
        // 最终状态校验
        assertEquals("v1", redisClient.get(key1));
        assertEquals("1", redisClient.get(key2));
        
        redisTemplate.delete(List.of(key1, key2));
        log.info("通用 Pipeline 功能测试通过");
    }
}
