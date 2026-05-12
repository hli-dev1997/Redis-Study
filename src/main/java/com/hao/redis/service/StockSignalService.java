package com.hao.redis.service;

import com.hao.redis.dal.model.StockSignal;

import java.time.LocalDate;
import java.util.List;

/**
 * 选股信号服务接口
 *
 * 类职责：
 * 定义选股信号的核心读写操作，屏蔽多级缓存与数据库访问细节。
 *
 * 缓存架构说明（L0→L1→L2→L3→DB）：
 * - L0 Bloom Filter：T+1 全量预热，过滤绝对不存在的 strategyId+tradeDate 组合（0误判率下放行）
 * - L1 Caffeine：本地缓存，expireAfterWrite 3s，maximumSize 1000
 * - L2 Redis Cluster：分布式缓存，物理 TTL 1800s±随机抖动（防雪崩）
 * - L3 Sentinel（DAO 层降级）：暂未启用，兜底 DB 保护预留点
 * - DB：MySQL 分区表，命中索引 idx_strategy_date_status
 *
 * 防护机制：
 * - 缓存穿透：Bloom Filter + 空值缓存（null cache，60s）
 * - 缓存击穿：DistributedLock tryLock(非阻塞) + 获锁失败短暂休眠再重试
 * - 缓存雪崩：随机 TTL 抖动 (baseTtl × [1, 1.1])
 * - 写后失效：@TransactionalEventListener(AFTER_COMMIT) + @Async 异步删除
 */
public interface StockSignalService {

    /**
     * 查询指定策略在某交易日的有效选股信号列表
     *
     * 读取链路：L0 Bloom → L1 Caffeine → 空值缓存 → L2 Redis → 分布式锁 → DB → 回填缓存
     *
     * @param strategyId 策略唯一标识（如 MOMENTUM_V2）
     * @param tradeDate  交易日期（如 2025-05-11）
     * @return 有效信号列表，按信号时间倒序；Bloom 过滤或 DB 无数据时返回空列表
     */
    List<StockSignal> querySignals(String strategyId, LocalDate tradeDate);

    /**
     * 保存新选股信号到数据库，并在事务提交后异步清除对应缓存
     *
     * 写链路：
     * 1. @Transactional 内执行 INSERT
     * 2. publishEvent(SignalSavedEvent) — 事件注册在事务上下文中
     * 3. 事务 COMMIT 成功后，AFTER_COMMIT 监听器异步删除 L1 + L2 缓存
     *
     * @param signal 待保存的信号实体（strategyId、tradeDate 必填）
     * @return 保存成功后的信号ID（数据库自增主键回写）
     */
    Long saveSignal(StockSignal signal);
}
