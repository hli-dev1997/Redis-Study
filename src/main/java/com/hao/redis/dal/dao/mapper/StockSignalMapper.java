package com.hao.redis.dal.dao.mapper;

import com.hao.redis.dal.model.StockSignal;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDate;
import java.util.List;

/**
 * 选股信号数据访问接口
 *
 * 类职责：
 * 提供 tb_quant_stock_signal 表的 CRUD 操作入口。
 *
 * 设计目的：
 * 1. 与多级缓存层配合，作为 L3（数据库兜底层）使用。
 * 2. 提供 T+1 布隆过滤器预热所需的全量键查询。
 *
 * 索引使用说明：
 * selectByStrategyAndDate 使用联合索引 idx_strategy_date_status
 * (strategy_id, trade_date, show_status)，覆盖最常见的查询路径。
 */
@Mapper
public interface StockSignalMapper {

    /**
     * 按策略ID与交易日查询有效信号列表
     *
     * 对应 SQL 索引：idx_strategy_date_status (strategy_id, trade_date, show_status)
     * 查询条件：strategy_id = ? AND trade_date = ? AND show_status = 1 AND status = 1
     *
     * @param strategyId 策略唯一标识
     * @param tradeDate  交易日期
     * @return 有效信号列表（按信号时间倒序）
     */
    List<StockSignal> selectByStrategyAndDate(
            @Param("strategyId") String strategyId,
            @Param("tradeDate") LocalDate tradeDate
    );

    /**
     * 查询所有未逻辑删除的策略+日期组合键（用于布隆过滤器 T+1 预热）
     *
     * 返回格式：strategyId:tradeDate（如 MOMENTUM_V2:2025-05-11）
     *
     * 【设计说明】只过滤 status=1，不过滤 show_status：
     * 布隆过滤器的职责是判断某个策略日期组合"是否存在于系统中"，
     * 与信号是否对外展示（show_status）无关。show_status 是业务可见性字段，
     * 由 selectByStrategyAndDate 负责过滤。若此处加 show_status=1，
     * 则 show_status=0 的记录不会进入 Bloom，对应查询将被 L0 永久误拦截。
     *
     * @return 所有未删除策略日期键列表
     */
    List<String> selectAllDistinctKeys();

    /**
     * 插入一条新的选股信号
     *
     * @param signal 信号实体
     * @return 影响行数
     */
    int insert(StockSignal signal);
}
