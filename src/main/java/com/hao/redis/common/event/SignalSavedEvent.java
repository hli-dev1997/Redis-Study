package com.hao.redis.common.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

import java.time.LocalDate;

/**
 * 选股信号落库完成事件
 *
 * 类职责：
 * 携带新信号的关键信息，在事务提交后触发缓存失效逻辑。
 *
 * 设计目的：
 * 配合 @TransactionalEventListener(AFTER_COMMIT) 使用，确保缓存删除
 * 操作只在数据库写入成功后执行，避免缓存与数据库之间出现 ABA 脏读。
 *
 * 使用场景：
 * StockSignalServiceImpl.saveSignal() 在 @Transactional 方法中发布此事件，
 * Spring 会在事务 COMMIT 后回调监听器 StockSignalServiceImpl.onSignalSaved()，
 * 由监听器异步删除 L1（Caffeine）和 L2（Redis）缓存。
 *
 * 与 Canal + Kafka 方案的区别：
 * - Canal 适合多消费者、高频写入的场景（消息队列解耦）。
 * - AFTER_COMMIT 适合单消费者、低频事件驱动的场景（延迟 < 10ms，运维成本极低）。
 */
@Getter
public class SignalSavedEvent extends ApplicationEvent {

    /** 信号所属策略ID */
    private final String strategyId;

    /** 信号所属交易日 */
    private final LocalDate tradeDate;

    /**
     * 构造选股信号落库事件
     *
     * @param source     事件来源（通常为发布事件的 Service 实例）
     * @param strategyId 策略唯一标识
     * @param tradeDate  所属交易日
     */
    public SignalSavedEvent(Object source, String strategyId, LocalDate tradeDate) {
        super(source);
        this.strategyId = strategyId;
        this.tradeDate = tradeDate;
    }
}
