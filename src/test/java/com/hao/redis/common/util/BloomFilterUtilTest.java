package com.hao.redis.common.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 布隆过滤器测试类（MurmurHash3 优化版）
 * <p>
 * 测试目的：
 * 1. 验证 MurmurHash3 优化后的布隆过滤器功能正确性。
 * 2. 测试误判率是否符合理论预期。
 * 3. 验证位分布均匀性。
 * <p>
 * 测试策略：
 * - 功能测试：验证添加和判断的基本逻辑。
 * - 误判率测试：统计实际误判率，与理论值对比。
 * - 边界测试：空值、特殊字符处理。
 */
@Slf4j
@SpringBootTest
public class BloomFilterUtilTest {

    @Autowired
    private BloomFilterUtil bloomFilterUtil;

    @Autowired
    private StringRedisTemplate redisTemplate;

    // 测试使用的业务分类
    private static final String TEST_CATEGORY = "test_murmur3";

    /**
     * 测试前清理数据
     */
    @BeforeEach
    public void setup() {
        log.info("测试前清理|Test_setup_cleanup");
        cleanupTestData();
    }

    /**
     * 测试后清理数据
     */
    @AfterEach
    public void tearDown() {
        log.info("测试后清理|Test_teardown_cleanup");
        cleanupTestData();
    }

    /**
     * 清理测试数据
     */
    private void cleanupTestData() {
        Set<String> keys = redisTemplate.keys("bloom:filter:" + TEST_CATEGORY + "*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    // ==========================================
    // 功能正确性测试
    // ==========================================

    @Test
    @DisplayName("功能测试：添加元素后能正确判断存在")
    public void testAddAndMightContain() {
        // Given: 准备测试数据
        String value1 = "user_001";
        String value2 = "user_002";
        String value3 = "user_003";

        // When: 添加元素
        bloomFilterUtil.add(TEST_CATEGORY, value1);
        bloomFilterUtil.add(TEST_CATEGORY, value2);
        bloomFilterUtil.add(TEST_CATEGORY, value3);

        // Then: 验证元素存在
        assertTrue(bloomFilterUtil.mightContain(TEST_CATEGORY, value1),
                "已添加的元素 value1 应该返回 true");
        assertTrue(bloomFilterUtil.mightContain(TEST_CATEGORY, value2),
                "已添加的元素 value2 应该返回 true");
        assertTrue(bloomFilterUtil.mightContain(TEST_CATEGORY, value3),
                "已添加的元素 value3 应该返回 true");

        log.info("功能测试通过|Function_test_passed,addedCount=3");
    }

    @Test
    @DisplayName("功能测试：未添加的元素返回不存在")
    public void testNotAddedElement() {
        // Given: 添加一些元素
        bloomFilterUtil.add(TEST_CATEGORY, "existing_user_1");
        bloomFilterUtil.add(TEST_CATEGORY, "existing_user_2");

        // When & Then: 验证未添加的元素返回 false
        // 注意：有极小概率误判，但在小数据量下几乎不可能
        assertFalse(bloomFilterUtil.mightContain(TEST_CATEGORY, "non_existing_user_xyz"),
                "未添加的元素应该返回 false");

        log.info("未添加元素测试通过|NotAdded_test_passed");
    }

    @Test
    @DisplayName("边界测试：空值处理")
    public void testNullValues() {
        // When & Then: null 值不应抛出异常
        assertDoesNotThrow(() -> bloomFilterUtil.add(TEST_CATEGORY, null),
                "添加 null 值不应抛出异常");
        assertDoesNotThrow(() -> bloomFilterUtil.add(null, "value"),
                "添加 null category 不应抛出异常");

        assertFalse(bloomFilterUtil.mightContain(TEST_CATEGORY, null),
                "判断 null 值应返回 false");
        assertFalse(bloomFilterUtil.mightContain(null, "value"),
                "判断 null category 应返回 false");

        log.info("空值处理测试通过|Null_handling_test_passed");
    }

    @Test
    @DisplayName("边界测试：特殊字符处理")
    public void testSpecialCharacters() {
        // Given: 包含特殊字符的值
        String[] specialValues = {
                "user@domain.com",           // 邮箱
                "中文用户名",                 // 中文
                "日本語ユーザー",              // 日语
                "emoji_🎉_test",              // emoji
                "special!@#$%^&*()",         // 特殊符号
                "   spaces   ",              // 空格
                ""                           // 空字符串
        };

        // When: 添加所有特殊值
        for (String value : specialValues) {
            if (value != null && !value.isEmpty()) {
                bloomFilterUtil.add(TEST_CATEGORY, value);
            }
        }

        // Then: 验证非空值都能正确判断
        for (String value : specialValues) {
            if (value != null && !value.isEmpty()) {
                assertTrue(bloomFilterUtil.mightContain(TEST_CATEGORY, value),
                        "特殊字符值 [" + value + "] 添加后应该返回 true");
            }
        }

        log.info("特殊字符处理测试通过|SpecialChar_test_passed,count={}", specialValues.length);
    }

    // ==========================================
    // 误判率测试
    // ==========================================

    @Test
    @DisplayName("误判率测试：验证 MurmurHash3 优化效果")
    public void testFalsePositiveRate() {
        // ==========================================
        // 测试参数配置
        // ==========================================
        // 插入元素数量：1 万
        int insertCount = 10_000;
        // 测试不存在元素数量：1 万
        int testCount = 10_000;
        // 期望误判率上限（MurmurHash3 优化后应该更低）
        double expectedMaxFalsePositiveRate = 0.01; // 1%

        log.info("误判率测试开始|FPR_test_start,insertCount={},testCount={}", insertCount, testCount);

        // ==========================================
        // 步骤1：插入已知元素
        // ==========================================
        Set<String> insertedElements = new HashSet<>();
        long insertStart = System.currentTimeMillis();
        for (int i = 0; i < insertCount; i++) {
            String element = "inserted_element_" + i;
            bloomFilterUtil.add(TEST_CATEGORY, element);
            insertedElements.add(element);
        }
        long insertCost = System.currentTimeMillis() - insertStart;
        log.info("元素插入完成|Insert_done,count={},costMs={}", insertCount, insertCost);

        // ==========================================
        // 步骤2：验证已插入元素（应该 100% 返回 true）
        // ==========================================
        int insertedMissCount = 0;
        for (String element : insertedElements) {
            if (!bloomFilterUtil.mightContain(TEST_CATEGORY, element)) {
                insertedMissCount++;
            }
        }
        assertEquals(0, insertedMissCount,
                "已插入的元素不应该有漏判（False Negative）");

        // ==========================================
        // 步骤3：测试未插入元素的误判率
        // ==========================================
        int falsePositiveCount = 0;
        long testStart = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            // 使用 UUID 确保与已插入元素不同
            String nonExistElement = "non_exist_" + UUID.randomUUID().toString();
            if (bloomFilterUtil.mightContain(TEST_CATEGORY, nonExistElement)) {
                falsePositiveCount++;
            }
        }
        long testCost = System.currentTimeMillis() - testStart;

        // ==========================================
        // 步骤4：计算并验证误判率
        // ==========================================
        double actualFalsePositiveRate = (double) falsePositiveCount / testCount;

        log.info("误判率测试完成|FPR_test_done,falsePositive={},testCount={},rate={},costMs={}",
                falsePositiveCount, testCount,
                String.format("%.4f%%", actualFalsePositiveRate * 100),
                testCost);

        // 验证误判率在预期范围内
        assertTrue(actualFalsePositiveRate <= expectedMaxFalsePositiveRate,
                String.format("实际误判率 %.4f%% 应小于预期上限 %.2f%%",
                        actualFalsePositiveRate * 100, expectedMaxFalsePositiveRate * 100));

        log.info("误判率测试通过|FPR_test_passed,actualRate={},expectedMax={}",
                String.format("%.4f%%", actualFalsePositiveRate * 100),
                String.format("%.2f%%", expectedMaxFalsePositiveRate * 100));
    }

    // ==========================================
    // 配置信息测试
    // ==========================================

    @Test
    @DisplayName("配置测试：获取布隆过滤器配置信息")
    public void testGetConfigInfo() {
        // When: 获取配置信息
        String configInfo = bloomFilterUtil.getConfigInfo();

        // Then: 验证配置信息包含关键参数
        assertNotNull(configInfo, "配置信息不应为 null");
        assertTrue(configInfo.contains("bitSize="), "配置信息应包含 bitSize");
        assertTrue(configInfo.contains("hashCount="), "配置信息应包含 hashCount");
        assertTrue(configInfo.contains("maxCapacity="), "配置信息应包含 maxCapacity");

        log.info("配置信息|Config_info={}", configInfo);
    }

    // ==========================================
    // 性能测试
    // ==========================================

    @Test
    @DisplayName("性能测试：MurmurHash3 吞吐量")
    public void testPerformance() {
        // 测试参数
        int operationCount = 10_000;

        // 1. 测试添加性能
        long addStart = System.currentTimeMillis();
        for (int i = 0; i < operationCount; i++) {
            bloomFilterUtil.add(TEST_CATEGORY, "perf_test_" + i);
        }
        long addCost = System.currentTimeMillis() - addStart;
        double addTps = (double) operationCount / (addCost / 1000.0);

        // 2. 测试查询性能
        long queryStart = System.currentTimeMillis();
        for (int i = 0; i < operationCount; i++) {
            bloomFilterUtil.mightContain(TEST_CATEGORY, "perf_test_" + i);
        }
        long queryCost = System.currentTimeMillis() - queryStart;
        double queryTps = (double) operationCount / (queryCost / 1000.0);

        log.info("性能测试完成|Performance_test_done,addCost={}ms,addTps={},queryCost={}ms,queryTps={}",
                addCost, String.format("%.0f", addTps),
                queryCost, String.format("%.0f", queryTps));

        // 验证性能在合理范围内（至少每秒 1000 次操作）
        assertTrue(addTps > 1000, "添加 TPS 应该大于 1000");
        assertTrue(queryTps > 1000, "查询 TPS 应该大于 1000");
    }
}
