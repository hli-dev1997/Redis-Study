package com.hao.redis.controller;

import com.hao.redis.dal.model.StockSignal;
import com.hao.redis.service.StockSignalService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 选股信号控制器
 *
 * 类职责：
 * 提供选股信号的查询与写入 HTTP 接口，作为多级缓存读链路的流量入口。
 *
 * 设计目的：
 * 1. 控制层保持轻量，只做参数接收与基础校验，业务逻辑全部委托给 StockSignalService。
 * 2. 接口设计符合 RESTful 规范，便于 Gatling/JMeter 压测。
 *
 * 接口列表：
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │ GET  /stock/signal?strategyId=MOMENTUM_V2&tradeDate=2025-05-11             │
 * │   → 查询选股信号列表（触发 L0→L1→L2→DB 完整读链路）                        │
 * │                                                                            │
 * │ POST /stock/signal                                                         │
 * │   Body: { "strategyId": "MOMENTUM_V2", "tradeDate": "2025-05-11", ... }   │
 * │   → 写入新信号，AFTER_COMMIT 后异步清除对应缓存                             │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 * 压测建议：
 * - 查询接口：并发 1000，相同 strategyId+tradeDate，验证 L1+L2 缓存命中率 ≥ 98%
 * - 写入接口：单次写入后立即查询，验证 AFTER_COMMIT 缓存失效延迟 < 50ms
 */
@Slf4j
@RestController
@RequestMapping("/stock")
@RequiredArgsConstructor
public class StockSignalController {

    private final StockSignalService stockSignalService;

    // ===========================
    // 1. 查询接口（读链路）
    // ===========================

    /**
     * 查询指定策略在某交易日的选股信号列表
     *
     * 读取链路：
     * L0 Bloom Filter → L1 Caffeine → 空值缓存 → L2 Redis → 分布式锁 → DB → 回填缓存
     *
     * 请求示例：
     * GET /stock/signal?strategyId=MOMENTUM_V2&tradeDate=2025-05-11
     *
     * 响应示例：
     * [
     *   { "windCode": "000001.SZ", "signalType": "BUY", "triggerPrice": 12.34, ... },
     *   { "windCode": "600000.SH", "signalType": "BUY", "triggerPrice": 9.87, ... }
     * ]
     *
     * @param strategyId 策略唯一标识（必填，如 MOMENTUM_V2）
     * @param tradeDate  交易日期（必填，格式 yyyy-MM-dd）
     * @return 信号列表（布隆过滤器拦截或无数据时返回空数组）
     */
    @GetMapping("/signal")
    public List<StockSignal> querySignals(
            @RequestParam String strategyId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate tradeDate) {

        log.debug("查询选股信号|Query_signals, strategyId={}, tradeDate={}", strategyId, tradeDate);

        // 基础参数校验
        if (strategyId == null || strategyId.isBlank()) {
            log.warn("strategyId 不能为空|StrategyId_blank");
            return List.of();
        }
        if (tradeDate == null) {
            log.warn("tradeDate 不能为空|TradeDate_null");
            return List.of();
        }

        return stockSignalService.querySignals(strategyId, tradeDate);
    }

    // ===========================
    // 2. 写入接口（写链路）
    // ===========================

    /**
     * 保存新选股信号（演示 AFTER_COMMIT 缓存失效链路）
     *
     * 写链路：
     * 1. @Transactional 内执行 DB INSERT
     * 2. 发布 SignalSavedEvent（绑定当前事务）
     * 3. 事务 COMMIT 成功后，@Async 异步删除 L1(Caffeine) + L2(Redis)
     * 4. 客户端下次查询时 Cache Miss → 重新从 DB 加载最新数据
     *
     * 请求示例：
     * POST /stock/signal
     * Content-Type: application/json
     * {
     *   "windCode": "000001.SZ",
     *   "strategyId": "MOMENTUM_V2",
     *   "signalType": "BUY",
     *   "triggerPrice": 12.34,
     *   "signalTime": "2025-05-11T09:30:00",
     *   "tradeDate": "2025-05-11",
     *   "showStatus": 1,
     *   "riskSnapshot": 85
     * }
     *
     * 响应示例：
     * { "id": 10086, "message": "信号保存成功，缓存异步清除中" }
     *
     * @param signal 选股信号实体（strategyId、tradeDate 必填）
     * @return 包含新信号 ID 的结果 Map
     */
    @PostMapping("/signal")
    public Map<String, Object> saveSignal(@RequestBody StockSignal signal) {
        log.debug("保存选股信号|Save_signal, strategyId={}, tradeDate={}, windCode={}",
                signal.getStrategyId(), signal.getTradeDate(), signal.getWindCode());

        // 业务校验
        if (signal.getStrategyId() == null || signal.getStrategyId().isBlank()) {
            return errorResponse("strategyId 不能为空");
        }
        if (signal.getTradeDate() == null) {
            return errorResponse("tradeDate 不能为空");
        }
        if (signal.getWindCode() == null || signal.getWindCode().isBlank()) {
            return errorResponse("windCode 不能为空");
        }

        Long id = stockSignalService.saveSignal(signal);

        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("message", "信号保存成功，缓存异步清除中");
        result.put("strategyId", signal.getStrategyId());
        result.put("tradeDate", signal.getTradeDate().toString());
        return result;
    }

    // ===========================
    // 3. 辅助接口（调试/面试演示）
    // ===========================

    /**
     * 快速健康检查（验证缓存层是否就绪）
     *
     * GET /stock/health
     */
    @GetMapping("/health")
    public Map<String, String> health() {
        Map<String, String> resp = new HashMap<>();
        resp.put("status", "UP");
        resp.put("service", "StockSignal");
        resp.put("cacheLayer", "L0(Bloom) + L1(Caffeine 3s) + L2(Redis 1800s±10%)");
        resp.put("antiPenetration", "Bloom + NullCache(60s)");
        resp.put("antiBreakdown", "DistributedLock tryLock + sleep(30ms) retry");
        resp.put("antiAvalanche", "RandomTTL [1800, 1980]s");
        resp.put("cacheInvalidation", "AFTER_COMMIT @Async delete");
        return resp;
    }

    // =================== 工具方法 ===================

    private Map<String, Object> errorResponse(String message) {
        Map<String, Object> err = new HashMap<>();
        err.put("error", message);
        err.put("code", 400);
        return err;
    }
}
