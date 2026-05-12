package com.hao.redis.common.monitor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis 监控指标统计类
 * <p>
 * 类职责：
 * 实时记录 Redis 操作的关键指标（如限流、锁状态、异常等），并定期输出到日志。
 * <p>
 * 设计目的：
 * 1. 提升系统的可观测性。
 * 2. 为容量规划和故障排查提供数据支撑。
 * <p>
 * 【性能调优 - 重点说明】
 * 1. 低损耗统计：使用 AtomicLong 进行内存级的原子统计，几乎不消耗业务执行性能。
 * 2. 指标闭环：通过监控限流命中率和缓存重建频率，开发人员可以实时感知系统的热点分布，进而动态调整 Redis 节点的分布策略。
 *
 * TODO: 未来可集成 Prometheus / Micrometer 导出到 Grafana。
 */
@Component
@Slf4j
public class RedisMetrics {

    // 限流命中计数器（成功通过）
    private final AtomicLong rateLimitHitCount = new AtomicLong(0);
    // 限流拦截计数器（拒绝请求）
    private final AtomicLong rateLimitBlockCount = new AtomicLong(0);
    // 缓存击穿重建计数器
    private final AtomicLong cacheRebuildCount = new AtomicLong(0);

    /**
     * 记录一次限流通过
     */
    public void recordRateLimitHit() {
        rateLimitHitCount.incrementAndGet();
    }

    /**
     * 记录一次限流拒绝
     */
    public void recordRateLimitBlock() {
        rateLimitBlockCount.incrementAndGet();
    }

    /**
     * 记录一次缓存重建
     */
    public void recordCacheRebuild() {
        cacheRebuildCount.incrementAndGet();
    }

//    /**
//     * 定时报告监控指标（每分钟一次）
//     * 输出结构：监控指标|Metrics,name=val,name=val
//     */
//    @Scheduled(fixedRate = 60000)
//    public void reportMetrics() {
//        long hits = rateLimitHitCount.getAndSet(0);
//        long blocks = rateLimitBlockCount.getAndSet(0);
//        long rebuilds = cacheRebuildCount.getAndSet(0);
//
//        // 计算限流拒绝率
//        double total = hits + blocks;
//        double blockRate = total > 0 ? (double) blocks / total * 100 : 0;
//
//        log.info("Redis监控指标报告|Redis_metrics_report, hits={}, blocks={}, blockRate={}% ,rebuilds={}",
//                hits, blocks, String.format("%.2f", blockRate), rebuilds);
//    }
}
