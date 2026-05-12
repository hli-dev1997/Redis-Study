package com.hao.redis.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hao.redis.common.enums.RedisKeysEnum;
import com.hao.redis.common.event.SignalSavedEvent;
import com.hao.redis.common.util.BloomFilterUtil;
import com.hao.redis.common.util.JsonUtil;
import com.hao.redis.dal.dao.mapper.StockSignalMapper;
import com.hao.redis.dal.model.StockSignal;
import com.hao.redis.integration.lock.DistributedLock;
import com.hao.redis.integration.lock.DistributedLockService;
import com.hao.redis.integration.redis.RedisClient;
import com.hao.redis.service.StockSignalService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 选股信号服务实现（亿级流量多级缓存架构）
 *
 * ┌─────────────────────────────────────────────────────┐
 * │                    读链路（L0 → DB）                 │
 * ├─────────┬───────────────────────────────────────────┤
 * │ L0      │ Bloom Filter（T+1 预热，防穿透）           │
 * │ L1      │ Caffeine 本地缓存（3s TTL，防击穿热读）    │
 * │ null    │ 空值缓存（60s TTL，防布隆误判穿透）        │
 * │ L2      │ Redis Cluster（1800s±10% 随机 TTL，防雪崩）│
 * │ lock    │ DistributedLock tryLock（防击穿并发回源）  │
 * │ DB      │ MySQL 分区表（兜底，命中联合索引）         │
 * └─────────┴───────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────┐
 * │                    写链路（缓存失效）                 │
 * ├─────────┬───────────────────────────────────────────┤
 * │ step 1  │ @Transactional INSERT 到 MySQL            │
 * │ step 2  │ publishEvent(SignalSavedEvent)             │
 * │ step 3  │ 事务 COMMIT 后 AFTER_COMMIT 回调          │
 * │ step 4  │ @Async 异步删除 L1(Caffeine) + L2(Redis)  │
 * └─────────┴───────────────────────────────────────────┘
 *
 * 为什么选 AFTER_COMMIT 而非 Canal + Kafka：
 * - 信号写入低频（每日策略计算一次），单消费者场景
 * - 无需引入额外中间件，延迟 < 10ms（Canal 方案约 20-50ms）
 * - 事务失败不会误删缓存（Canal 无法感知事务成功与否）
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StockSignalServiceImpl implements StockSignalService {

    // =================== 依赖注入 ===================
    private final RedisClient<String>        redisClient;
    private final BloomFilterUtil            bloomFilterUtil;
    private final DistributedLockService     lockService;
    private final StockSignalMapper          signalMapper;
    private final ApplicationEventPublisher  eventPublisher;

    // =================== 常量定义 ===================

    /** 布隆过滤器业务分类键 */
    private static final String BLOOM_CATEGORY = "stock:signal";

    /** Redis L2 缓存基础 TTL（秒）= 30 分钟 */
    private static final int BASE_TTL_SECONDS = 1800;

    /** 随机抖动系数：TTL 在 BASE ～ BASE×1.1 之间随机，防止集中过期雪崩 */
    private static final double JITTER_RATIO = 0.1;

    /** 空值缓存 TTL（秒）= 1 分钟，防止布隆误判导致穿透 */
    private static final int NULL_CACHE_TTL = 60;

    /** 获锁失败后的短暂休眠时间（毫秒）：等待持锁线程完成 DB 回源并写入缓存 */
    private static final int LOCK_FAIL_SLEEP_MS = 30;

    // =================== 日志采样计数器 ===================
    /**
     * L1 Caffeine 命中计数器：每 100 次命中打印一条 INFO 日志。
     * 高并发下 L1 全命中会产生海量日志，采样避免淹没关键事件。
     */
    private final AtomicLong l1HitCounter = new AtomicLong(0);

    /**
     * L2 Redis 命中计数器：每 10 次命中打印一条 INFO 日志。
     * L2 命中频率低于 L1，采样间隔相应缩短。
     */
    private final AtomicLong l2HitCounter = new AtomicLong(0);

    /** L1 日志采样间隔：每 1000 次命中打印 1 次 */
    private static final long L1_LOG_SAMPLE = 1000;

    /** L2 日志采样间隔：每 10 次命中打印 1 次 */
    private static final long L2_LOG_SAMPLE = 10;

    // =================== L1 本地缓存 ===================
    /**
     * Caffeine L1 缓存
     *
     * 设计参数：
     * - expireAfterWrite 3s：与 Redis L2 TTL 相比极短，保证数据一致性窗口可控
     * - maximumSize 1000：选股策略种类有限，1000 条足够覆盖热点
     * - 存储 JSON 字符串：与 Redis 格式保持一致，序列化/反序列化逻辑复用
     *
     * 为什么不用 Spring @Cacheable：
     * @Cacheable 对 ConcurrentHashMap 或 CaffeineCache 封装，
     * 但在击穿防护场景中我们需要手动控制"锁+缓存回填"的顺序，
     * 不适合用注解驱动的声明式缓存。
     */
    private final Cache<String, String> l1Cache = Caffeine.newBuilder()
            .expireAfterWrite(3, TimeUnit.SECONDS)
            .maximumSize(1000)
            .recordStats() // 开启统计，可通过 l1Cache.stats() 查看命中率
            .build();

    // =================== 初始化：布隆过滤器 T+1 预热 ===================

    /**
     * 应用启动后，将数据库中所有有效的 strategy_id:trade_date 组合
     * 批量写入布隆过滤器（T+1 全量预热）。
     *
     * 作用：
     * - 所有合法键已在 Bloom 中 → mightContain 返回 true → 正常走后续链路
     * - 攻击者构造的随机策略ID → mightContain 返回 false → 直接拒绝，不打 DB
     *
     * 时机说明：
     * @PostConstruct 在 Spring Bean 完全初始化后执行，此时 Mapper 已可用。
     * 实际生产中可以用 XXL-JOB 每天凌晨 T+1 触发一次全量刷新。
     */
    @PostConstruct
    public void warmUpBloomFilter() {
        log.info("布隆过滤器T+1预热开始|BloomFilter_warmup_start");
        try {
            List<String> keys = signalMapper.selectAllDistinctKeys();
            if (keys == null || keys.isEmpty()) {
                log.warn("布隆过滤器预热数据为空|BloomFilter_warmup_empty_result");
                return;
            }
            keys.forEach(key -> bloomFilterUtil.add(BLOOM_CATEGORY, key));
            log.info("布隆过滤器T+1预热完成|BloomFilter_warmup_done, keyCount={}", keys.size());
        } catch (Exception e) {
            // 预热失败不影响启动，降级为所有请求走后续链路
            log.error("布隆过滤器预热失败_降级放行全部请求|BloomFilter_warmup_fail", e);
        }
    }

    // =================== 读链路：L1 → L0 → null → L2 → lock → DB ===================

    /**
     * 查询选股信号（完整多级缓存读链路）
     *
     * 正确顺序：L1(纯内存) → L0 Bloom(Redis) → null cache(Redis) → L2(Redis) → lock → DB
     *
     * 关键设计：L1 必须在 Bloom 之前。
     * Bloom filter 需要 5 次 Redis GETBIT，有网络开销。
     * 热点 key 在 L1 命中率 > 99%，若 Bloom 在前，每个请求都白跑 5 次 Redis，
     * 200 并发下产生 1000 个并发 Redis 调用，直接撑爆连接池，引发慢日志洪泛。
     */
    @Override
    public List<StockSignal> querySignals(String strategyId, LocalDate tradeDate) {
        String bizKey = buildBizKey(strategyId, tradeDate);

        // ────────────────────────────────────────────────
        // L1：Caffeine 本地缓存（纯 JVM 内存，零网络开销，最优先）
        // 热点 key 在 3s TTL 内全部命中此层，99%+ 请求到此结束
        // ────────────────────────────────────────────────
        String l1Json = l1Cache.getIfPresent(bizKey);
        if (l1Json != null) {
            // 采样日志：每 100 次命中打印一次，避免高并发下日志洪泛
            long hits = l1HitCounter.incrementAndGet();
            if (hits % L1_LOG_SAMPLE == 0) {
                log.info("L1_Caffeine采样命中|L1_hit_sampled, bizKey={}, totalHits={}", bizKey, hits);
            }
            return deserializeSignals(l1Json);
        }

        // ────────────────────────────────────────────────
        // L0：布隆过滤器（L1 未命中后才查，防穿透第一道防线）
        // 返回 false = 一定不存在（100% 准确），直接拒绝，不打 Redis/DB
        // 返回 true  = 可能存在（有极低误判率），继续走后续链路
        // 放在 L1 之后：避免热点 key 每次都产生 5 次 Redis GETBIT
        // ────────────────────────────────────────────────
        if (!bloomFilterUtil.mightContain(BLOOM_CATEGORY, bizKey)) {
            log.warn("L0_布隆过滤器拦截_Key不存在|Bloom_miss, strategyId={}, tradeDate={}", strategyId, tradeDate);
            return Collections.emptyList();
        }

        // ────────────────────────────────────────────────
        // 空值缓存检查（布隆误判兜底）
        // 若之前 DB 查询结果为空，写了空值标记，直接返回空列表
        // 避免布隆误判的 Key 反复打穿到 DB
        // ────────────────────────────────────────────────
        String nullCacheKey = RedisKeysEnum.STOCK_SIGNAL_NULL_CACHE.join(bizKey);
        if (Boolean.TRUE.equals(redisClient.exists(nullCacheKey))) {
            log.warn("空值缓存命中_拦截布隆误判|Null_cache_hit, bizKey={}", bizKey);
            return Collections.emptyList();
        }

        // ────────────────────────────────────────────────
        // L2：Redis Cluster 分布式缓存
        // 命中率目标 ≥ 38%（覆盖 L1 未命中部分，L1+L2 合计 ≥ 98%）
        // ────────────────────────────────────────────────
        String redisKey = RedisKeysEnum.STOCK_SIGNAL_CACHE.join(bizKey);
        String redisJson = redisClient.get(redisKey);
        if (redisJson != null) {
            // 采样日志：每 10 次命中打印一次
            long hits = l2HitCounter.incrementAndGet();
            if (hits % L2_LOG_SAMPLE == 0) {
                log.info("L2_Redis采样命中|L2_hit_sampled, bizKey={}, totalHits={}", bizKey, hits);
            }
            l1Cache.put(bizKey, redisJson);
            return deserializeSignals(redisJson);
        }
        // L2 未命中是关键事件（需要抢锁回源），每次必打
        log.info("L2_Redis未命中_进入抢锁回源|L2_cache_miss, bizKey={}", bizKey);

        // ────────────────────────────────────────────────
        // 分布式锁防击穿（互斥锁方案）
        // 只有第一个拿到锁的线程才会查 DB，其余线程短暂等待后重读缓存
        // 相比逻辑过期：物理 TTL 策略在 AFTER_COMMIT 删除 Key 后无旧值可返回，
        //               所以必须用互斥锁而非逻辑过期
        // ────────────────────────────────────────────────
        return loadFromDbWithLock(bizKey, redisKey, nullCacheKey, strategyId, tradeDate);
    }

    /**
     * 加锁回源 DB，防止缓存击穿时大量并发请求同时打到数据库
     */
    private List<StockSignal> loadFromDbWithLock(
            String bizKey, String redisKey, String nullCacheKey,
            String strategyId, LocalDate tradeDate) {

        // 每个 bizKey 独享一把锁（锁粒度细化到策略+日期）
        String lockName = RedisKeysEnum.STOCK_SIGNAL_LOCK.join(bizKey);
        DistributedLock lock = lockService.getLock(lockName);
        boolean acquired = false;

        try {
            // tryLock() 非阻塞：获锁成功 → 查 DB；获锁失败 → 等待后重读缓存
            acquired = lock.tryLock();

            if (!acquired) {
                // 获锁失败：说明已有线程在查 DB，短暂休眠后重读 L1/L2
                // 通常 DB 查询 + 缓存写入 < 20ms，休眠 30ms 足够
                log.info("获取分布式锁失败_等待缓存回填|Lock_fail_wait, bizKey={}", bizKey);
                Thread.sleep(LOCK_FAIL_SLEEP_MS);

                // double-check L1
                String l1Json = l1Cache.getIfPresent(bizKey);
                if (l1Json != null) {
                    return deserializeSignals(l1Json);
                }
                // double-check L2
                String redisJson = redisClient.get(redisKey);
                if (redisJson != null) {
                    l1Cache.put(bizKey, redisJson);
                    return deserializeSignals(redisJson);
                }
                // 仍未命中，降级返回空列表（极端情况：持锁线程查 DB 也未找到数据）
                log.warn("等待缓存回填后仍未命中_降级返回|Lock_fail_degrade, bizKey={}", bizKey);
                return Collections.emptyList();
            }

            // ─── 获锁成功：double-check L2（防止重复查 DB）───
            // 场景：多个线程同时到达锁等待，第一个释放锁后，
            //       后续线程可能在 acquired=true 之后才读 L2，必须再检查一次
            String redisJsonAfterLock = redisClient.get(redisKey);
            if (redisJsonAfterLock != null) {
                log.debug("获锁后L2_double_check命中|Lock_acquired_L2_hit, bizKey={}", bizKey);
                l1Cache.put(bizKey, redisJsonAfterLock);
                return deserializeSignals(redisJsonAfterLock);
            }

            // ─── 查 DB（L3 兜底层）───
            log.info("回源数据库|DB_fallback, strategyId={}, tradeDate={}", strategyId, tradeDate);
            List<StockSignal> dbResult = signalMapper.selectByStrategyAndDate(strategyId, tradeDate);

            if (dbResult == null || dbResult.isEmpty()) {
                // DB 也没有数据：写空值缓存（60s），防止布隆误判导致的反复穿透
                log.warn("DB查询结果为空_写入空值缓存|DB_empty_write_null_cache, bizKey={}", bizKey);
                redisClient.setex(nullCacheKey, NULL_CACHE_TTL, "1");
                return Collections.emptyList();
            }

            // ─── 回填 L2 Redis（随机 TTL 防雪崩）───
            // 随机 TTL = baseTtl × (1 + random(0, 0.1))
            // 例：baseTtl=1800s → 实际 TTL 在 1800~1980s 之间均匀分布
            long jitterSeconds = (long) (BASE_TTL_SECONDS * JITTER_RATIO * ThreadLocalRandom.current().nextDouble());
            int finalTtl = (int) (BASE_TTL_SECONDS + jitterSeconds);
            String resultJson = JsonUtil.toJson(dbResult);
            redisClient.setex(redisKey, finalTtl, resultJson);
            log.info("回填L2_Redis完成|L2_cache_write, bizKey={}, ttl={}s", bizKey, finalTtl);

            // ─── 回填 L1 Caffeine ───
            l1Cache.put(bizKey, resultJson);

            // ─── 新 Key 补充写入布隆过滤器（T+1 预热可能未覆盖当日最新数据）───
            bloomFilterUtil.add(BLOOM_CATEGORY, bizKey);

            return dbResult;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("等待分布式锁被中断|Lock_wait_interrupted, bizKey={}", bizKey, e);
            return Collections.emptyList();
        } finally {
            // 只有本线程持有锁才释放（防止误解锁其他线程的锁）
            if (acquired) {
                lock.unlock();
                log.debug("分布式锁已释放|Lock_released, bizKey={}", bizKey);
            }
        }
    }

    // =================== 写链路：@Transactional + AFTER_COMMIT 缓存失效 ===================

    /**
     * 保存选股信号（写链路入口）
     *
     * 核心设计：
     * 1. @Transactional 保证 DB 写入的原子性
     * 2. publishEvent 将 SignalSavedEvent 注册在当前事务上下文
     * 3. Spring 在事务 COMMIT 后回调 onSignalSaved()
     * 4. @Async 保证缓存删除在独立线程异步执行，不阻塞 HTTP 响应
     *
     * 为什么是"删除"而非"更新"缓存：
     * - 避免 ABA 问题：并发更新时 setex 可能写入旧数据覆盖新数据
     * - Cache-Aside 最佳实践：写 DB → 删缓存 → 下次读时重建
     *
     * @param signal 待保存信号（strategyId、tradeDate 必填）
     * @return 自增主键 ID
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Long saveSignal(StockSignal signal) {
        // 补全默认值
        if (signal.getStatus() == null) {
            signal.setStatus(1);
        }
        if (signal.getShowStatus() == null) {
            signal.setShowStatus(1);
        }

        // DB 写入（MyBatis useGeneratedKeys 回写 id 到 signal.id）
        signalMapper.insert(signal);
        log.info("选股信号落库成功|Signal_saved, id={}, strategyId={}, tradeDate={}",
                signal.getId(), signal.getStrategyId(), signal.getTradeDate());

        // 发布事件：注册在事务上下文，COMMIT 后才会触发监听器
        // 若事务回滚，事件不会触发，不会产生"DB 没写成功但缓存被删"的问题
        eventPublisher.publishEvent(new SignalSavedEvent(this, signal.getStrategyId(), signal.getTradeDate()));

        return signal.getId();
    }

    /**
     * 事务提交后异步清除缓存（AFTER_COMMIT 缓存失效监听器）
     *
     * 执行时机：事务 COMMIT 成功后（DB 数据已持久化）
     * 执行线程：@Async 在 ioTaskExecutor 线程池中异步执行，不阻塞业务线程
     *
     * 失败处理：
     * - 删除 Redis 失败 → 记录 warn 日志，等待 L2 TTL 自然到期（最多 1980s）
     * - 删除 L1 失败    → Caffeine 3s TTL 自然到期后自动失效
     * - 两种降级都不影响数据最终一致性
     *
     * @param event 信号落库完成事件
     */
    @Async("ioTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onSignalSaved(SignalSavedEvent event) {
        String bizKey = buildBizKey(event.getStrategyId(), event.getTradeDate());
        log.info("AFTER_COMMIT_缓存失效开始|Cache_invalidate_start, bizKey={}", bizKey);

        // 1. 删除 L1 Caffeine（本地缓存，直接失效）
        l1Cache.invalidate(bizKey);
        log.debug("L1_Caffeine缓存已删除|L1_invalidated, bizKey={}", bizKey);

        // 2. 删除 L2 Redis（分布式缓存）
        String redisKey = RedisKeysEnum.STOCK_SIGNAL_CACHE.join(bizKey);
        try {
            redisClient.del(redisKey);
            log.info("L2_Redis缓存删除成功|L2_invalidated, bizKey={}, key={}", bizKey, redisKey);
        } catch (Exception e) {
            // 删除失败不抛异常，等待 TTL 自然过期（最终一致性保障）
            log.warn("L2_Redis缓存删除失败_等待TTL自然过期|L2_invalidate_fail, bizKey={}", bizKey, e);
        }

        // 3. 同步更新布隆过滤器（新数据一定存在，补充写入）
        bloomFilterUtil.add(BLOOM_CATEGORY, bizKey);
    }

    // =================== 工具方法 ===================

    /**
     * 构建业务缓存键
     *
     * 格式：strategyId:tradeDate（如 MOMENTUM_V2:2025-05-11）
     * 与布隆过滤器键、Redis Key 后缀、L1 Key 保持一致
     */
    private String buildBizKey(String strategyId, LocalDate tradeDate) {
        return strategyId + ":" + tradeDate;
    }

    /**
     * 反序列化 JSON 字符串为信号列表
     *
     * @param json JSON 字符串
     * @return StockSignal 列表
     */
    private List<StockSignal> deserializeSignals(String json) {
        List<StockSignal> result = JsonUtil.toList(json, StockSignal.class);
        return result != null ? result : Collections.emptyList();
    }
}
