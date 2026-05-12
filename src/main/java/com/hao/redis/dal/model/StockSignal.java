package com.hao.redis.dal.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 量化选股信号实体
 *
 * 类职责：
 * 映射数据库表 tb_quant_stock_signal，承载单条选股信号的完整数据。
 *
 * 设计目的：
 * 1. 统一字段定义，为多级缓存与持久化层提供一致的数据载体。
 * 2. 支持 JSON 序列化，以便在 Redis 中以字符串形式存储。
 *
 * 表结构对应：
 * - wind_code      -> windCode   (股票代码，如 000001.SZ)
 * - strategy_id    -> strategyId (策略唯一标识)
 * - signal_type    -> signalType (买入/卖出，BUY/SELL)
 * - trigger_price  -> triggerPrice (触发时价格)
 * - signal_time    -> signalTime   (信号触发时间，精确到毫秒)
 * - trade_date     -> tradeDate    (所属交易日)
 * - show_status    -> showStatus   (1=展示，0=隐藏)
 * - risk_snapshot  -> riskSnapshot (风控快照分数)
 * - status         -> status       (1=有效，0=删除)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockSignal {

    /** 主键 */
    private Long id;

    /** 股票代码（Wind 格式，如 000001.SZ） */
    private String windCode;

    /** 策略唯一标识（如 MOMENTUM_V2） */
    private String strategyId;

    /** 信号类型（BUY / SELL） */
    private String signalType;

    /** 信号触发时的股票价格 */
    private BigDecimal triggerPrice;

    /** 信号产生时间，精确到毫秒 */
    private LocalDateTime signalTime;

    /** 所属交易日（T 日） */
    private LocalDate tradeDate;

    /** 展示状态：1=展示，0=隐藏（风控后置处理） */
    private Integer showStatus;

    /** 风控快照分数（-100~100，正数越高越安全） */
    private Integer riskSnapshot;

    /** 记录状态：1=有效，0=逻辑删除 */
    private Integer status;
}
