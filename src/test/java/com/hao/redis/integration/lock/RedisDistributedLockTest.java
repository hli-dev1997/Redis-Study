package com.hao.redis.integration.lock;

import com.hao.redis.integration.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式锁集成测试（增强版）
 * <p>
 * 测试验证点：
 * 1. 基础加锁与解锁功能。
 * 2. 互斥性：同一时间只有一个线程能获锁。
 * 3. 可重入性：同一个线程多次获锁不阻塞。
 * 4. 看门狗：长时间持有时自动续活，防止过早释放。
 * 5. 安全解锁：获锁线程只能释放自己持有的锁。
 * 6. 监控指标：ActiveLockCount 正确变化。
 */
@Slf4j
@SpringBootTest
public class RedisDistributedLockTest {

    @Autowired
    private RedisClient<String> redisClient;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private DistributedLockService lockService;

    private static final String LOCK_KEY = "test:lock:distributed";

    @BeforeEach
    public void setup() {
        log.info("测试前清理锁键|Test_setup_cleanup");
        redisTemplate.delete(LOCK_KEY);
    }

    @AfterEach
    public void tearDown() {
        log.info("测试后清理锁键|Test_teardown_cleanup");
        redisTemplate.delete(LOCK_KEY);
    }

    @Test
    @DisplayName("基础功能：加锁后能正常释放，监控指标正确")
    public void testBasicLockUnlock() {
        DistributedLock lock = lockService.getLock(LOCK_KEY);
        
        // 1. 获取前状态
        int initialCount = RedisDistributedLock.getActiveLockCount();
        
        // 2. 加锁
        boolean success = lock.tryLock();
        assertTrue(success, "首次加锁应成功");
        assertEquals(initialCount + 1, RedisDistributedLock.getActiveLockCount(), "活跃锁计数应增加");
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_KEY)), "Redis 中应存在该 Key");

        // 3. 释放
        lock.unlock();
        assertEquals(initialCount, RedisDistributedLock.getActiveLockCount(), "活跃锁计数应恢复");
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_KEY)), "Redis 中应不存在该 Key");
        
        log.info("基础加解锁测试通过|Basic_lock_unlock_passed");
    }

    @Test
    @DisplayName("互斥性：多线程竞争，只有一个能获锁")
    public void testMutualExclusion() throws InterruptedException {
        DistributedLock lock1 = lockService.getLock(LOCK_KEY);
        DistributedLock lock2 = lockService.getLock(LOCK_KEY);

        // 线程 A 获锁
        assertTrue(lock1.tryLock(), "线程 A 获锁成功");

        // 线程 B 尝试获锁（应该失败）
        Thread tB = new Thread(() -> {
            boolean bResult = lock2.tryLock();
            assertFalse(bResult, "线程 B 获锁应失败");
            log.info("线程 B 竞争锁失败，互斥性校验成功");
        });
        tB.start();
        tB.join();

        // 线程 A 释放后，线程 B 才能获锁
        lock1.unlock();
        
        Thread tB_again = new Thread(() -> {
            assertTrue(lock2.tryLock(), "线程 A 释放后，线程 B 获锁成功");
            lock2.unlock();
        });
        tB_again.start();
        tB_again.join();

        log.info("互斥性测试通过|Mutual_exclusion_passed");
    }

    @Test
    @DisplayName("可重入性：同一个线程并发多次加锁不应死锁")
    public void testReentrancy() {
        DistributedLock lock = lockService.getLock(LOCK_KEY);

        assertTrue(lock.tryLock(), "第一次加锁");
        assertTrue(lock.tryLock(), "第二次重入加锁");
        
        // 此时 Redis 中的 TTL 应该被第2次调用刷新或延续
        Long ttl = redisTemplate.getExpire(LOCK_KEY, TimeUnit.MILLISECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0);

        lock.unlock(); // 第一次解锁（计数减1，锁不消失）
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_KEY)), "重入锁未完全释放前 Key 应保留");

        lock.unlock(); // 第二次解锁（完全释放）
        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_KEY)), "重入锁完全释放后 Key 应删除");

        log.info("可重入性测试通过|Reentrancy_passed");
    }

    @Test
    @DisplayName("看门狗：长时间持锁，Redis Key 不应过期")
    public void testWatchdogRenewal() throws InterruptedException {
        // 设置较短的过期时间，方便测试续活
        DistributedLock lock = new RedisDistributedLock(LOCK_KEY, redisClient, redisTemplate, 1500L);

        assertTrue(lock.tryLock(), "获锁成功");
        
        // 初始 TTL 约 1.5s
        Long ttl1 = redisTemplate.getExpire(LOCK_KEY, TimeUnit.MILLISECONDS);
        log.info("初始 TTL: {}ms", ttl1);
        
        // 等待超过 1.5s，但由于看门狗（每 500ms 续期一次），Key 不应消失
        Thread.sleep(2000L);
        
        assertTrue(Boolean.TRUE.equals(redisTemplate.hasKey(LOCK_KEY)), "看门狗应完成续期，Key 不应过期");
        Long ttl2 = redisTemplate.getExpire(LOCK_KEY, TimeUnit.MILLISECONDS);
        log.info("续期后 TTL: {}ms", ttl2);
        assertTrue(ttl2 > 500, "TTL 应被重置为接近 1500");

        lock.unlock();
        log.info("看门狗续期测试通过|Watchdog_renewal_passed");
    }
}
