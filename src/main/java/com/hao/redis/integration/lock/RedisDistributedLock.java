package com.hao.redis.integration.lock;

import com.hao.redis.integration.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 Redis 的分布式锁实现（含看门狗优化版）
 * <p>
 * 类职责：
 * 提供具备并发安全性、重入性以及自动续期支持的分布式锁。
 * <p>
 * 设计目的：
 * 1. 互斥性：利用 Redis SET NX 保证同一时刻只有一个线程获锁。
 * 2. 容错性：通过过期时间保证持有锁的进程宕机后锁能自动释放。
 * 3. 灵活性：支持看门狗续期，解决业务执行时间超过预设过期时间的问题。
 * 4. 高并发支持：优化看门狗线程池，提升大规模锁续活请求的处理效率。
 * <p>
 * 核心优化点：
 * - 线程池调优：将单线程 ScheduledExecutor 升级为核心线程数与 CPU 相关的 ScheduledThreadPool，避免高负载下续活延迟。
 * - 监控增强：引入 AtomicInteger 实时统计集群本节点活跃的锁数量。
 * - 代码规范：补充详细的实现注释，统一日志风格。
 * <p>
 * 【性能调优 - 重点说明】
 * 1. 线程模型优化：原生实现使用单线程处理所有锁的续约，在大规模锁场景下（如万级并发）
 *    会导致续约任务排队堆积，锁意外失效。升级后的多线程 ScheduledThreadPool 显著提升了续活并发力。
 * 2. 交互开销：使用 Lua 脚本合并“校验-删除”和“校验-续活”操作，减少网络 RTT 和并发冲突风险。
 * 3. 监控闭环：通过 AtomicInteger 实现轻量级计数，提供秒级的锁活跃指标，辅助高并发下的系统扩容决策。
 */
@Slf4j
public class RedisDistributedLock implements DistributedLock {

    private final String lockKey;
    private final RedisClient<String> redisClient;
    private final StringRedisTemplate stringRedisTemplate;
    private final long lockWatchdogTimeout;

    // 线程本地变量，存储当前线程持有的锁 Value（UUID+ThreadId），用于安全解锁
    private final ThreadLocal<String> threadLockValue = new ThreadLocal<>();
    // 线程本地变量，存储锁重入计数
    private final ThreadLocal<Integer> threadLockCount = ThreadLocal.withInitial(() -> 0);
    // 线程本地变量，存储当前线程负责的看门狗任务句柄
    private final ThreadLocal<ScheduledFuture<?>> watchdogTask = new ThreadLocal<>();

    /**
     * 监控指标：当前节点活跃的分布式锁总数
     */
    private static final AtomicInteger ACTIVE_LOCK_COUNT = new AtomicInteger(0);

    /**
     * 优化点：使用多线程调度执行器
     * 通过 ScheduledThreadPool 提升高并发场景下看门狗续期的及时性。
     */
    private static final ScheduledExecutorService WATCHDOG_EXECUTOR = Executors.newScheduledThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            runnable -> {
                Thread thread = new Thread(runnable, "RedisLockWatchdog");
                thread.setDaemon(true); // 设置为守护线程，随 JVM 退出
                return thread;
            }
    );

    /**
     * 构造函数
     *
     * @param lockKey             锁的唯一标识
     * @param redisClient         Redis 封装客户端
     * @param stringRedisTemplate 原生 Spring Data Redis 模板（用于 Lua 脚本执行）
     * @param lockWatchdogTimeout 锁的默认过期时间（毫秒），也是看门狗续期的基准
     */
    public RedisDistributedLock(String lockKey, RedisClient<String> redisClient, StringRedisTemplate stringRedisTemplate, long lockWatchdogTimeout) {
        this.lockKey = lockKey;
        this.redisClient = redisClient;
        this.stringRedisTemplate = stringRedisTemplate;
        this.lockWatchdogTimeout = lockWatchdogTimeout;
    }

    @Override
    public void lock() {
        // 阻塞加锁逻辑：循环重试直至成功
        // 在生产环境建议增加最大重试时间限制，此处演示核心逻辑
        while (!tryLock()) {
            try {
                // 等待 50ms 后重试，减少对 Redis 的 CPU 消耗
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("分布式锁阻塞加锁被中断|Lock_wait_interrupted, key={}", lockKey);
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        // 核心逻辑：可重入性校验
        if (threadLockCount.get() > 0) {
            threadLockCount.set(threadLockCount.get() + 1);
            log.debug("分布式锁重入成功|Lock_reentrant_success, key={}, currentCount={}", lockKey, threadLockCount.get());
            return true;
        }

        // 核心逻辑：尝试获锁
        String lockValue = generateLockValue();
        // 底层调用 SET lockKey lockValue NX PX lockWatchdogTimeout
        Boolean success = redisClient.tryLock(lockKey, lockValue, lockWatchdogTimeout, TimeUnit.MILLISECONDS);

        if (Boolean.TRUE.equals(success)) {
            // 获锁成功，记录本地状态
            threadLockValue.set(lockValue);
            threadLockCount.set(1);
            
            // 监控指标增加
            int currentTotal = ACTIVE_LOCK_COUNT.incrementAndGet();
            
            // 启动看门狗续期
            startWatchdog();
            
            log.info("分布式锁加锁成功|Lock_acquired_success, key={}, activeLocks={}", lockKey, currentTotal);
            return true;
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long waitTime = unit.toMillis(time);

        // 核心逻辑：带超时的锁竞争
        while (System.currentTimeMillis() - startTime < waitTime) {
            if (tryLock()) {
                return true;
            }
            Thread.sleep(50);
        }
        log.warn("分布式锁竞争超时|Lock_acquire_timeout, key={}, waitTime={}ms", lockKey, waitTime);
        return false;
    }

    @Override
    public void unlock() {
        // 核心逻辑：前置校验
        if (threadLockCount.get() == 0) {
            log.warn("释放未持有锁警告|Unlock_not_held_warning, key={}", lockKey);
            return;
        }

        // 核心逻辑：重入计数递减
        threadLockCount.set(threadLockCount.get() - 1);

        if (threadLockCount.get() > 0) {
            log.debug("分布式锁重入释放_计数减一|Lock_reentrant_unlock, key={}, remainingCount={}", lockKey, threadLockCount.get());
            return;
        }

        // 核心逻辑：彻底释放锁
        try {
            // 1. 停止看门狗
            stopWatchdog();
            
            // 2. 通过 Lua 脚本安全释放锁（校验 Value 是否匹配，防止误删他人的锁）
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                            "return redis.call('del', KEYS[1]) " +
                            "else return 0 end";
            
            Long result = stringRedisTemplate.execute(
                    new DefaultRedisScript<>(script, Long.class),
                    Collections.singletonList(lockKey),
                    threadLockValue.get()
            );

            if (Long.valueOf(1).equals(result)) {
                int remainingTotal = ACTIVE_LOCK_COUNT.decrementAndGet();
                log.info("分布式锁释放成功|Lock_released_success, key={}, activeLocks={}", lockKey, remainingTotal);
            } else {
                log.error("分布式锁释放失败_Value不匹配|Lock_released_fail_mismatch, key={}", lockKey);
            }
        } catch (Exception e) {
            log.error("分布式锁释放异常|Lock_released_error, key={}", lockKey, e);
        } finally {
            // 核心逻辑：清理本地资源，防止线程复用导致的内存泄露或逻辑错误
            threadLockValue.remove();
            threadLockCount.remove();
            watchdogTask.remove();
        }
    }

    /**
     * 启动看门狗
     * 周期性对锁进行续活，续活频率为过期时间的 1/3。
     */
    private void startWatchdog() {
        long renewalInterval = lockWatchdogTimeout / 3;
        final String lockValue = threadLockValue.get();

        ScheduledFuture<?> future = WATCHDOG_EXECUTOR.scheduleAtFixedRate(() -> {
            try {
                // Lua 脚本：当 Key 对应 Value 仍然是当前线程持有的 Value 时，重置过期时间
                String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                                "return redis.call('pexpire', KEYS[1], ARGV[2]) " +
                                "else return 0 end";
                
                Long result = stringRedisTemplate.execute(
                        new DefaultRedisScript<>(script, Long.class),
                        Collections.singletonList(lockKey),
                        lockValue,
                        String.valueOf(lockWatchdogTimeout)
                );

                if (Long.valueOf(1).equals(result)) {
                    log.debug("看门狗续期成功|Watchdog_renew_success, key={}", lockKey);
                } else {
                    log.warn("看门狗续期失败_可能锁已释放或被夺取|Watchdog_renew_fail, key={}", lockKey);
                    // 如果续费失败且锁不属于自己了，主动停止续活任务
                    throw new IllegalStateException("Watchdog renewal failed: Lock not held.");
                }
            } catch (Exception e) {
                log.error("看门狗续期执行异常|Watchdog_renew_exception, key={}", lockKey, e.getMessage());
                // 这里选择取消任务，具体策略可根据业务容忍度调整
                throw new RuntimeException(e);
            }
        }, renewalInterval, renewalInterval, TimeUnit.MILLISECONDS);
        
        watchdogTask.set(future);
    }

    /**
     * 停止看门狗任务
     */
    private void stopWatchdog() {
        ScheduledFuture<?> future = watchdogTask.get();
        if (future != null && !future.isDone()) {
            future.cancel(true);
            log.debug("看门狗任务停止成功|Watchdog_task_stopped, key={}", lockKey);
        }
    }

    /**
     * 生成锁的值
     * 格式：UUID + 线程ID，由于 RedisDistributedLock 本身实例可能被多个线程共用，
     * 必须保证 Value 的全局唯一性。
     */
    private String generateLockValue() {
        return UUID.randomUUID().toString() + ":" + Thread.currentThread().getId();
    }

    /**
     * 获取当前节点活跃锁数量（用于辅助监控）
     */
    public static int getActiveLockCount() {
        return ACTIVE_LOCK_COUNT.get();
    }
}

