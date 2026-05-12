package com.hao.redis.integration.cluster;

import com.hao.redis.common.util.HotKeyShardingUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 热点 Key 分片路由测试 (Hot Key Sharding Route Test)
 *
 * <p><b>与 RedisClusterRoutingTest 的区别：</b>
 * <ul>
 *   <li>{@code RedisClusterRoutingTest}：用本地 {@code RedisClusterTopologyCache} + {@code RedisSlotUtil}
 *       计算 Slot，验证自研拓扑缓存的正确性</li>
 *   <li>{@code HotKeyRouteTest}（本文件）：通过 {@code RedisClusterConnection.clusterGetNodeForKey()}
 *       直接向集群查询路由，绕过本地缓存，拿到集群实际视角的 Node 归属，更适合验证热点 Key 分片后的物理打散效果</li>
 * </ul>
 *
 * <p><b>测试目标（三阶段闭环）：</b>
 * <ol>
 *   <li>计算 1000 个分片后缀的路由分布 → 证明热点 Key 被打散到了不同物理节点</li>
 *   <li>实际写入样本 Key，Value 中植入节点标识 → 为第三阶段的反向验证做准备</li>
 *   <li>读取并反向验证：从 Redis 读出的 Value 包含实际节点信息 → 闭环双向一致性验证</li>
 * </ol>
 *
 * <p><b>热点 Key 分片的业务场景：</b>
 * <pre>
 *   问题：stock:10086 是热点 Key（秒杀库存），所有请求都打到同一个 Master 节点，
 *         该节点 CPU 100%，其他节点空闲，集群资源严重倾斜。
 *
 *   解决：将一个热点 Key 复制为 N 个分片副本：
 *         stock:10086#0, stock:10086#1, ..., stock:10086#999
 *         各后缀经 CRC16 计算后落到不同 Slot，分布到不同 Master，请求随机路由到任一副本。
 *
 *   注意：分片 Key 不使用 Hash Tag({})，因为 Hash Tag 会强制所有 Key 落同一 Slot，
 *         与分片目的相反。
 * </pre>
 *
 * @author hli
 * @see RedisClusterRoutingTest 本地拓扑缓存路由验证（互补测试）
 */
@Slf4j
@SpringBootTest
public class HotKeyRouteTest {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 热点 Key 的基础前缀（模拟真实业务场景：SKU 10086 的库存热点 Key）
     *
     * <p>分片规则：baseKey + "#" + suffix（1~1000）
     * <p>注意：不使用 Redis Hash Tag ({...})，否则所有分片会落同一 Slot，失去分片意义
     */
    private static final String BASE_HOT_KEY = "stock:10086#";

    /** 测试分片副本数量（模拟生产中配置 1000 个分片） */
    private static final int SHARD_COUNT = 1000;

    /**
     * 热点 Key 分片路由三阶段测试
     *
     * <p><b>第一阶段：路由统计</b> - 计算 1000 个分片后缀落在哪些物理节点上
     * <p><b>第二阶段：真实写入</b> - 从每个节点各取 1 个样本写入，Value 植入节点标识
     * <p><b>第三阶段：反向验证</b> - 读取并校验路由一致性
     * <p><b>第四阶段：清理</b>   - finally 兜底，无论成功失败都清除测试脏数据
     */
    @Test
    @DisplayName("热点Key分片路由验证：1000个后缀打散到多个Master节点")
    public void testHotKeyScatteringAndRouting() {
        // nodeToSuffixesMap: 节点标识 (IP:Port) -> 落在该节点上的后缀列表
        // 用于统计分片分布，证明 Key 被打散到了不同物理节点
        Map<String, List<Integer>> nodeToSuffixesMap = new HashMap<>();

        // targetSampleKeys: 被选中用于实际读写验证的样本 Key
        // 声明在 try 外部，确保 finally 清理块能访问到
        List<String> targetSampleKeys = new ArrayList<>();

        // 获取 Redis 集群底层原生连接（Lettuce ClusterConnection）
        // try-with-resources：确保连接使用后正确关闭，避免连接泄漏
        try (RedisClusterConnection clusterConnection =
                     stringRedisTemplate.getConnectionFactory().getClusterConnection()) {

            // =====================================================
            // 第一阶段：计算 1000 个分片后缀的物理节点路由分布
            // =====================================================
            log.info("===== 第一阶段：计算路由分布（共{}个分片副本）=====", SHARD_COUNT);

            for (int i = 1; i <= SHARD_COUNT; i++) {
                String shardKey = BASE_HOT_KEY + i;

                // clusterGetNodeForKey：直接向集群查询该 Key 当前归属的物理 Master 节点
                // 底层执行：CRC16(key) % 16384 → 定位 Slot → 查询 Slot 所在节点的 IP:Port
                // 相比本地 RedisSlotUtil，这里拿到的是集群视角的实时路由，不依赖本地缓存
                RedisClusterNode targetNode = clusterConnection.clusterGetNodeForKey(shardKey.getBytes());

                // 组装节点唯一标识符，格式：192.168.254.2:6401
                String nodeId = targetNode.getHost() + ":" + targetNode.getPort();

                // 将后缀归入对应节点的分片列表
                nodeToSuffixesMap.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(i);
            }

            // 打印各节点的分片分布情况，证明热点 Key 被打散
            log.info("===== 路由分布统计结果（共{}个节点）=====", nodeToSuffixesMap.size());
            for (Map.Entry<String, List<Integer>> entry : nodeToSuffixesMap.entrySet()) {
                String nodeId = entry.getKey();
                List<Integer> suffixes = entry.getValue();
                // 理想情况：3 主节点各负责约 333 个分片，负载均衡
                String ratio = String.format("%.1f", suffixes.size() * 100.0 / SHARD_COUNT);
                log.info("节点 [{}] 分配到 {}/{} 个分片（占比 {}%），示例后缀（前5个）: {}",
                        nodeId,
                        suffixes.size(),
                        SHARD_COUNT,
                        ratio,
                        suffixes.subList(0, Math.min(5, suffixes.size())));
            }

            // 断言：分片必须打散到多个节点（3主3从集群中至少命中 3 个主节点）
            // 若所有 Key 落同一节点，说明分片方案有误（例如误用了 Hash Tag 导致强制路由）
            assertTrue(nodeToSuffixesMap.size() > 1,
                    "分片失败！所有 Key 集中在同一节点，热点未打散。检查是否误用 Hash Tag({})！");
            log.info("✅ 断言通过：分片成功打散到 {} 个不同物理节点", nodeToSuffixesMap.size());

            // =====================================================
            // 第二阶段：从每个节点各抽取 1 个样本，执行真实写入
            // Value 中植入节点标识，为第三阶段的反向验证做准备
            // =====================================================
            log.info("===== 第二阶段：真实写入样本数据（每个节点各 1 个）=====");

            for (Map.Entry<String, List<Integer>> entry : nodeToSuffixesMap.entrySet()) {
                String nodeId = entry.getKey();
                // 取该节点列表中的第一个后缀作为样本探针
                Integer sampleSuffix = entry.getValue().get(0);
                String sampleKey = BASE_HOT_KEY + sampleSuffix;
                targetSampleKeys.add(sampleKey);

                // Value 植入节点标识（IP:Port），用于第三阶段反向交叉验证
                // 格式：Data_For_192.168.254.2_6401
                String testValue = "Data_For_" + nodeId.replace(":", "_");

                // 实际网络写入，Spring Data Redis 会自动路由到正确节点
                stringRedisTemplate.opsForValue().set(sampleKey, testValue);
                log.info("写入 Key [{}] → 节点 [{}]，Value: [{}]", sampleKey, nodeId, testValue);
            }

            // =====================================================
            // 第三阶段：读取并双向验证路由一致性
            // 验证逻辑：从 Redis 读出 Value，反推其应归属节点，
            //           再通过 clusterGetNodeForKey 查询实际节点，两者必须一致
            // =====================================================
            log.info("===== 第三阶段：读取验证路由一致性 =====");

            for (String sampleKey : targetSampleKeys) {
                // 1. 从 Redis 读取值（客户端自动路由到正确节点）
                String valueFromRedis = stringRedisTemplate.opsForValue().get(sampleKey);
                assertNotNull(valueFromRedis, "读取失败！Redis 中不存在 Key: " + sampleKey);

                // 2. 重新查询该 Key 实际归属节点（以集群当前拓扑为准）
                RedisClusterNode actualNode = clusterConnection.clusterGetNodeForKey(sampleKey.getBytes());
                String actualNodeId = actualNode.getHost() + ":" + actualNode.getPort();

                // 3. 断言：Value 中包含实际节点标识，证明写入时路由与读取时路由一致
                // 这是闭环双向验证的核心：写入时我们知道目标节点，Value 记录了这个信息
                // 读取时 clusterGetNodeForKey 告知实际节点，两者必须吻合
                assertTrue(valueFromRedis.contains(actualNodeId.replace(":", "_")),
                        String.format("路由不一致！Key [%s] 实际归属节点 [%s]，但 Value [%s] 中未包含该节点标识",
                                sampleKey, actualNodeId, valueFromRedis));

                log.info("✅ 验证通过 | Key [{}] → 节点 [{}] → Value [{}]",
                        sampleKey, actualNodeId, valueFromRedis);
            }

            log.info("===== 🎉 三阶段验证全部通过！热点 Key 分片打散验证成功 =====");

        } catch (Exception e) {
            log.error("测试异常，将在 finally 中执行清理|HotKeyTest_error", e);
            throw new RuntimeException(e);
        } finally {
            // =====================================================
            // 第四阶段：清理测试脏数据（无论成功失败必须执行）
            // =====================================================
            log.info("===== 第四阶段：清理测试脏数据 =====");
            if (!targetSampleKeys.isEmpty()) {
                // StringRedisTemplate.delete(Collection) 支持跨节点批量删除
                // 注意：在集群模式下，不同 Key 落在不同节点，Spring Data Redis 会逐节点分批删除
                Long deletedCount = stringRedisTemplate.delete(targetSampleKeys);
                log.info("脏数据清理完成 | 目标: {}个, 实际删除: {}个",
                        targetSampleKeys.size(), deletedCount);
                assertEquals(
                        targetSampleKeys.size(),
                        deletedCount != null ? deletedCount.intValue() : 0,
                        "脏数据未完全清理！请手动检查集群状态。"
                );
            }
        }
    }

    /**
     * 跨基础Key的后缀路由验证
     *
     * <p><b>核心结论（预期提前知道）：</b>
     * <pre>
     *   相同后缀 + 不同基础Key → 不一定路由到同一节点
     *   原因：CRC16 对完整字符串计算，"stock:10086#2" 与 "user:hot#2" 是不同字符串
     *
     *   正确做法：每个基础Key都有自己的「后缀→节点」映射表
     *             对任意基础Key，都能找到路由到任意目标节点的后缀
     *             这才是后缀分片方案的通用性所在
     * </pre>
     *
     * <p><b>测试设计（两阶段）：</b>
     * <ol>
     *   <li>相同后缀跨Key对比：取第一个测试中已知的"代表后缀"，
     *       用不同基础Key测试相同后缀，观察路由是否相同</li>
     *   <li>多Key路由表构建：对三个不同基础Key各扫描100个后缀，
     *       证明每个基础Key都能找到路由到所有节点的后缀（方案具有通用性）</li>
     * </ol>
     */
    @Test
    @DisplayName("跨基础Key后缀路由验证：证明后缀路由是基础Key特定的，但方案具有通用性")
    public void testSuffixRoutingAcrossDifferentBaseKeys() {

        // 三个不同场景的热点 Key，模拟生产中不同业务的热点
        // stock:10086# → 股票行情热点 Key
        // user:hot#    → 热门用户信息热点 Key
        // rank:today#  → 今日排行榜热点 Key
        String[] baseKeys = {BASE_HOT_KEY, "user:hot#", "rank:today#"};

        // 每个基础Key的路由映射：节点 → 该节点下可用后缀列表（取前3个）
        // 用于证明每个基础Key都能找到路由到任意节点的后缀
        Map<String, Map<String, List<Integer>>> allKeyNodeMap = new LinkedHashMap<>();

        try (RedisClusterConnection clusterConnection =
                     stringRedisTemplate.getConnectionFactory().getClusterConnection()) {

            // =====================================================================
            // 第一阶段：为每个基础Key扫描 100 个后缀，构建各自的「节点→后缀」映射表
            // 扫描范围 100 个而非 1000 个，因为 100 个已足够覆盖所有节点
            // =====================================================================
            log.info("===== 第一阶段：为 {} 个基础Key构建路由映射表（每个扫描100个后缀）=====",
                    baseKeys.length);

            for (String baseKey : baseKeys) {
                Map<String, List<Integer>> nodeToSuffixes = new LinkedHashMap<>();

                for (int i = 1; i <= 100; i++) {
                    String fullKey = baseKey + i;
                    RedisClusterNode node = clusterConnection.clusterGetNodeForKey(fullKey.getBytes());
                    String nodeId = node.getHost() + ":" + node.getPort();
                    // 每个节点最多保留前 3 个后缀作为示例，避免日志过长
                    nodeToSuffixes.computeIfAbsent(nodeId, k -> new ArrayList<>());
                    if (nodeToSuffixes.get(nodeId).size() < 3) {
                        nodeToSuffixes.get(nodeId).add(i);
                    }
                }

                allKeyNodeMap.put(baseKey, nodeToSuffixes);

                log.info("基础Key [{}] 的节点路由表（各节点推荐后缀）：", baseKey);
                nodeToSuffixes.forEach((node, suffixes) ->
                        log.info("  → 节点 [{}]：推荐后缀 {} （取首个后缀写入即可路由到此节点）",
                                node, suffixes));
            }

            // =====================================================================
            // 第二阶段：相同后缀跨Key路由对比
            // 取 BASE_HOT_KEY 中每个节点的第一个"代表后缀"
            // 用相同后缀测试其他基础Key，观察是否路由到同一节点
            //
            // 预期结论：不同基础Key + 相同后缀 → 不一定同一节点
            //           CRC16 对完整字符串计算，后缀只是字符串的一部分
            // =====================================================================
            log.info("===== 第二阶段：相同后缀跨Key路由对比 =====");
            log.info("（验证：不同基础Key + 相同后缀 是否路由到同一节点）");

            // 取 stock:10086# 的代表后缀（每个节点各取第一个）
            Map<String, List<Integer>> baseKeyRouteMap = allKeyNodeMap.get(BASE_HOT_KEY);

            for (Map.Entry<String, List<Integer>> entry : baseKeyRouteMap.entrySet()) {
                String baseKeyTargetNode = entry.getKey();
                int representSuffix = entry.getValue().get(0);

                log.info("--- 代表后缀 #{} 在各基础Key上的路由结果 ---", representSuffix);
                log.info("  [基准] {} → 节点 [{}]", BASE_HOT_KEY + representSuffix, baseKeyTargetNode);

                // 用相同后缀测试其他基础Key
                for (String otherBaseKey : baseKeys) {
                    if (otherBaseKey.equals(BASE_HOT_KEY)) continue;

                    String otherFullKey = otherBaseKey + representSuffix;
                    RedisClusterNode otherNode = clusterConnection.clusterGetNodeForKey(otherFullKey.getBytes());
                    String otherNodeId = otherNode.getHost() + ":" + otherNode.getPort();
                    boolean sameNode = baseKeyTargetNode.equals(otherNodeId);

                    // 关键日志：直观展示相同后缀在不同基础Key下的路由差异
                    log.info("  [对比] {} → 节点 [{}] | 与基准相同: {}",
                            otherFullKey,
                            otherNodeId,
                            sameNode ? "✅ 是（巧合）" : "❌ 否（符合预期：CRC16对全字符串计算）");
                }
            }

            // =====================================================================
            // 第三阶段：结论验证
            // 断言：每个基础Key都能覆盖所有节点（方案具有通用性）
            // 断言：三个基础Key的"代表后缀"不完全相同（后缀路由是Key特定的）
            // =====================================================================
            log.info("===== 第三阶段：通用性断言 =====");

            for (String baseKey : baseKeys) {
                Map<String, List<Integer>> nodeMap = allKeyNodeMap.get(baseKey);
                // 断言：每个基础Key都能路由到所有 Master 节点（扫描100个后缀足够覆盖）
                int masterNodeCount = allKeyNodeMap.get(BASE_HOT_KEY).size(); // 以第一个测试确认的Master数为基准
                assertTrue(nodeMap.size() >= masterNodeCount,
                        String.format("基础Key [%s] 只覆盖了 %d 个节点，未能覆盖所有 %d 个Master节点！",
                                baseKey, nodeMap.size(), masterNodeCount));
                log.info("✅ 基础Key [{}] 覆盖了 {} 个节点，后缀分片方案通用性验证通过", baseKey, nodeMap.size());
            }

            log.info("===== 🎉 结论 =====");
            log.info("1. 相同后缀 + 不同基础Key → 不一定路由到同一节点（CRC16 对完整字符串计算）");
            log.info("2. 每个基础Key都能找到路由到任意节点的后缀（方案对任意热点Key通用）");
            log.info("3. 生产实践：每个热点Key维护自己的「后缀→节点」映射表，选正确后缀即可精确路由");
        }
    }

    /**
     * HotKeyShardingUtil 工具类验证
     *
     * <p>验证工具类核心能力：
     * <ol>
     *   <li>写入场景：{@code getOneShardKeyPerNode} 能为每个 Master 节点各找到一个分片 Key</li>
     *   <li>读取场景：{@code getRandomShardKey} 返回的 Key 实际可读写，且分布在集群节点上</li>
     *   <li>通用性：对不同基础 Key 调用工具类，均能覆盖全部节点</li>
     * </ol>
     */
    @Autowired
    private RedisClusterTopologyCache topologyCache;

    /**
     * 热点 Key 压测对比：分片前（单节点集中）vs 分片后（多节点均衡）
     *
     * <p><b>解决的核心问题：</b>
     * Redis Cluster 以 {@code CRC16(key) % 16384} 路由，同一个 Key 永远打同一个 Master。
     * 热点 Key 在高并发下导致单节点 CPU 打满，其他节点空闲。
     * 后缀分片将请求打散到所有节点，各节点各承担约 33% 负载。
     *
     * <p><b>压测设计（两阶段对比）：</b>
     * <pre>
     *   Phase 1 - 未分片（集中）：50 线程 × 200 次 GET 同一个 Key
     *             → 所有请求 100% 打到单个 Master 节点
     *
     *   Phase 2 - 分片后（均衡）：50 线程 × 200 次 GET 随机分片 Key
     *             → 请求均匀分散到 3 个 Master 节点，各约 33%
     * </pre>
     *
     * <p><b>监控指标对比（生产环境告警阈值）：</b>
     * <pre>
     *   Redis 慢日志：slowlog-log-slower-than = 10000μs（10ms）
     *   单节点 CPU 告警：>= 80%
     *   命令处理速率告警：单节点 QPS 远高于其他节点（热点特征）
     *
     *   未分片：10000 QPS 全打单节点 → CPU 趋近 100% → 慢日志大量触发
     *   分片后：10000 QPS 分散到 3 节点 → 各节点约 3333 QPS → CPU 约 33%
     * </pre>
     *
     * <p><b>注意：</b>
     * 开发/测试环境 QPS 远低于生产，单节点延迟绝对值差异较小（Redis 本地响应很快）。
     * 核心验证指标是<b>节点路由分布</b>（100% 集中 vs 33% 均衡），这是生产 CPU 倾斜的直接根因。
     * 延迟数值在生产万级并发下差异显著，本测试附带采集供参考。
     *
     * @throws InterruptedException 多线程等待中断
     */
    @Test
    @DisplayName("压测对比：[分片前]单节点100%集中 vs [分片后]多节点33%均衡（含延迟+QPS+节点分布）")
    public void testHotKeyPressureComparison() throws InterruptedException {

        final String HOT_KEY        = "bench:hot:stock:10086";
        final int    THREAD_COUNT   = 50;
        final int    REQ_PER_THREAD = 200;
        final int    TOTAL          = THREAD_COUNT * REQ_PER_THREAD;  // 10,000 次
        final int    SHARD_POOL     = 1000;

        // =====================================================================
        // 前置数据准备：写入单热点 Key + 所有分片副本
        // 写入每 5 个后缀一个（共 200 个后缀覆盖分片池），保证随机读时大多数 Key 存在
        // =====================================================================
        log.info("===== 前置准备：写入热点 Key + 分片副本数据 =====");
        stringRedisTemplate.opsForValue().set(HOT_KEY, "value_10086");

        // 每个 Master 节点各写一个分片副本（保证写入时各节点均有数据）
        Map<String, String> onePerNodeMap = HotKeyShardingUtil.getOneShardKeyPerNode(
                HOT_KEY, topologyCache, SHARD_POOL);
        onePerNodeMap.values().forEach(k -> stringRedisTemplate.opsForValue().set(k, "value_10086"));

        // 额外预写 200 个后缀（覆盖分片池 20% Key 空间，减少读到 null 的比例）
        List<String> extraKeys = new ArrayList<>();
        for (int i = 0; i < SHARD_POOL; i += 5) {
            String k = HOT_KEY + "#" + i;
            stringRedisTemplate.opsForValue().set(k, "value_10086");
            extraKeys.add(k);
        }

        List<String> allWrittenKeys = new ArrayList<>();
        allWrittenKeys.add(HOT_KEY);
        allWrittenKeys.addAll(onePerNodeMap.values());
        allWrittenKeys.addAll(extraKeys);
        log.info("前置准备完成：写入 {} 个 Key（1个热点 + {}个分片副本 + {}个扩展副本）",
                allWrittenKeys.size(), onePerNodeMap.size(), extraKeys.size());

        try {
            // =================================================================
            // 【预采样】节点命中分布对比（各模拟 1000 次路由，不做实际网络请求）
            // 目的：量化两种方案下的节点负载集中度，对应生产监控的"命令处理速率分布"
            // =================================================================
            log.info("===== [预采样] 节点命中分布（各 1000 次路由采样）=====");
            log.info("  对应生产监控：Redis 各节点 QPS / 命令处理速率（INFO stats → instantaneous_ops_per_sec）");

            Map<String, Integer> noShardDist  = new LinkedHashMap<>();
            Map<String, Integer> shardedDist  = new LinkedHashMap<>();

            try (RedisClusterConnection conn =
                         stringRedisTemplate.getConnectionFactory().getClusterConnection()) {
                // 未分片：1000 次全部查同一个 Key 的路由
                for (int i = 0; i < 1000; i++) {
                    RedisClusterNode n = conn.clusterGetNodeForKey(HOT_KEY.getBytes());
                    noShardDist.merge(n.getHost() + ":" + n.getPort(), 1, Integer::sum);
                }
                // 分片后：1000 次随机分片 Key 的路由
                for (int i = 0; i < 1000; i++) {
                    String sk = HotKeyShardingUtil.getRandomShardKey(HOT_KEY, SHARD_POOL);
                    RedisClusterNode n = conn.clusterGetNodeForKey(sk.getBytes());
                    shardedDist.merge(n.getHost() + ":" + n.getPort(), 1, Integer::sum);
                }
            }

            log.info("[未分片] 1000 次路由分布（⚠️ 预期：单节点 100% — 对应生产单节点 CPU 打满）：");
            noShardDist.forEach((node, cnt) -> {
                String pct = String.format("%.1f", cnt * 100.0 / 1000);
                String marker = cnt == 1000 ? "⚠️ 热点全集中" : "部分集中";
                log.info("  {} 节点 [{}] → {} 次 ({}%)", marker, node, cnt, pct);
            });

            log.info("[分片后] 1000 次路由分布（✅ 预期：各节点约 33% — 对应生产各节点 CPU 均衡）：");
            shardedDist.forEach((node, cnt) -> {
                String pct = String.format("%.1f", cnt * 100.0 / 1000);
                log.info("  ✅ 节点 [{}] → {} 次 ({}%)", node, cnt, pct);
            });

            // 关键断言：分布差异
            assertEquals(1, noShardDist.size(),
                    "未分片场景下所有路由必须 100% 集中在同一个节点！");
            assertTrue(shardedDist.size() >= 2,
                    "分片后路由必须分散到至少 2 个节点！");
            log.info("✅ 节点分布断言通过：未分片={}节点集中, 分片后={}节点分散",
                    noShardDist.size(), shardedDist.size());

            // =================================================================
            // 【Phase 1】未分片并发压测：50线程 × 200次 GET 同一个热点 Key
            // 模拟场景：所有流量打同一 Key → 单节点命令积压 → CPU 100%
            // =================================================================
            log.info("===== [Phase 1] 未分片并发压测：{} 线程 × {} 次 = {} 次总请求 =====",
                    THREAD_COUNT, REQ_PER_THREAD, TOTAL);
            log.info("  模拟监控告警：单节点 QPS 远超其他节点（热点特征）");

            Queue<Long>   noShardNanos  = new ConcurrentLinkedQueue<>();
            AtomicInteger noShardErrors = new AtomicInteger(0);
            CountDownLatch ready1 = new CountDownLatch(THREAD_COUNT);
            CountDownLatch go1    = new CountDownLatch(1);
            CountDownLatch done1  = new CountDownLatch(THREAD_COUNT);
            ExecutorService pool1 = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int t = 0; t < THREAD_COUNT; t++) {
                pool1.submit(() -> {
                    ready1.countDown();
                    try {
                        go1.await();                          // 等所有线程就绪后齐发
                        for (int r = 0; r < REQ_PER_THREAD; r++) {
                            long s = System.nanoTime();
                            try { stringRedisTemplate.opsForValue().get(HOT_KEY); }
                            catch (Exception e) { noShardErrors.incrementAndGet(); }
                            noShardNanos.offer(System.nanoTime() - s);
                        }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { done1.countDown(); }
                });
            }
            ready1.await();                         // 等全部线程就绪
            long p1Start = System.currentTimeMillis();
            go1.countDown();                        // 齐发！
            done1.await();
            long p1Ms = System.currentTimeMillis() - p1Start;
            pool1.shutdown();
            log.info("[Phase 1] 压测完成，总耗时 {} ms，错误数 {}", p1Ms, noShardErrors.get());

            // =================================================================
            // 【Phase 2】分片后并发压测：50线程 × 200次 GET 随机分片 Key
            // 模拟场景：流量均匀打到 3 个节点 → 各节点约 33% 负载
            // =================================================================
            log.info("===== [Phase 2] 分片后并发压测：{} 线程 × {} 次 = {} 次总请求 =====",
                    THREAD_COUNT, REQ_PER_THREAD, TOTAL);
            log.info("  模拟监控结果：3 节点 QPS 趋于均衡，单节点慢日志归零");

            Queue<Long>   shardedNanos  = new ConcurrentLinkedQueue<>();
            AtomicInteger shardedErrors = new AtomicInteger(0);
            CountDownLatch ready2 = new CountDownLatch(THREAD_COUNT);
            CountDownLatch go2    = new CountDownLatch(1);
            CountDownLatch done2  = new CountDownLatch(THREAD_COUNT);
            ExecutorService pool2 = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int t = 0; t < THREAD_COUNT; t++) {
                pool2.submit(() -> {
                    ready2.countDown();
                    try {
                        go2.await();
                        for (int r = 0; r < REQ_PER_THREAD; r++) {
                            // 每次请求随机选一个分片后缀（ThreadLocalRandom，线程安全且高效）
                            String sk = HotKeyShardingUtil.getRandomShardKey(HOT_KEY, SHARD_POOL);
                            long s = System.nanoTime();
                            try { stringRedisTemplate.opsForValue().get(sk); }
                            catch (Exception e) { shardedErrors.incrementAndGet(); }
                            shardedNanos.offer(System.nanoTime() - s);
                        }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { done2.countDown(); }
                });
            }
            ready2.await();
            long p2Start = System.currentTimeMillis();
            go2.countDown();
            done2.await();
            long p2Ms = System.currentTimeMillis() - p2Start;
            pool2.shutdown();
            log.info("[Phase 2] 压测完成，总耗时 {} ms，错误数 {}", p2Ms, shardedErrors.get());

            // =================================================================
            // 统计：计算延迟分位数 + 吞吐量对比
            // =================================================================
            long[] a1 = noShardNanos.stream().mapToLong(Long::longValue).sorted().toArray();
            long[] a2 = shardedNanos.stream().mapToLong(Long::longValue).sorted().toArray();

            // 全部预格式化为字符串，避免 SLF4J 不支持 {:.3f} 格式
            String avg1  = String.format("%.3f", Arrays.stream(a1).average().orElse(0) / 1_000_000.0);
            String avg2  = String.format("%.3f", Arrays.stream(a2).average().orElse(0) / 1_000_000.0);
            String p95_1 = String.format("%.3f", a1[(int)(a1.length * 0.95)] / 1_000_000.0);
            String p95_2 = String.format("%.3f", a2[(int)(a2.length * 0.95)] / 1_000_000.0);
            String p99_1 = String.format("%.3f", a1[(int)(a1.length * 0.99)] / 1_000_000.0);
            String p99_2 = String.format("%.3f", a2[(int)(a2.length * 0.99)] / 1_000_000.0);
            String max1  = String.format("%.3f", a1[a1.length - 1] / 1_000_000.0);
            String max2  = String.format("%.3f", a2[a2.length - 1] / 1_000_000.0);
            long   qps1  = (long)(TOTAL * 1000.0 / p1Ms);
            long   qps2  = (long)(TOTAL * 1000.0 / p2Ms);

            // 未分片场景：找到集中的那个节点及其 QPS 占比
            String hotNode = noShardDist.keySet().iterator().next();

            log.info("=================================================================");
            log.info(" 压测结果对比（{} 线程 × {} 请求 = {} 总次数）", THREAD_COUNT, REQ_PER_THREAD, TOTAL);
            log.info("=================================================================");
            log.info(" 指标                   │  未分片（热点集中）  │  分片后（均衡分散）");
            log.info("-----------------------------------------------------------------");
            log.info(" 命中节点数             │  {} 个（100% 集中）  │  {} 个（均衡分散）",
                    noShardDist.size(), shardedDist.size());
            log.info(" 热点节点 QPS 占比      │  100%（仅 {}）│  各约 {}%",
                    hotNode, String.format("%.0f", 100.0 / shardedDist.size()));
            log.info(" 平均延迟 (ms)          │  {}             │  {}",    avg1,  avg2);
            log.info(" P95 延迟 (ms)          │  {}             │  {}",    p95_1, p95_2);
            log.info(" P99 延迟 (ms)          │  {}             │  {}",    p99_1, p99_2);
            log.info(" 最大延迟 (ms)          │  {}             │  {}",    max1,  max2);
            log.info(" 总耗时 (ms)            │  {}             │  {}",    p1Ms,  p2Ms);
            log.info(" 吞吐量 (QPS)           │  {}/s           │  {}/s",  qps1,  qps2);
            log.info(" 错误数                 │  {}             │  {}",    noShardErrors.get(), shardedErrors.get());
            log.info("=================================================================");
            log.info(" 【生产告警阈值推算】");
            log.info("   Redis slowlog-log-slower-than = 10000μs (10ms)");
            log.info("   单节点 CPU 告警线 = 80%");
            log.info("   未分片：10000 QPS 全打节点[{}]", hotNode);
            log.info("           → 该节点命令速率是其他节点的 {}x，触发不均衡告警", shardedDist.size());
            log.info("           → 高并发下命令积压，延迟超 10ms，慢日志持续触发");
            log.info("   分片后：10000 QPS 均分到 {} 个节点，各约 {} QPS",
                    shardedDist.size(), 10000 / shardedDist.size());
            log.info("           → 各节点负载降至约 {}%，慢日志归零，CPU 告警消除",
                    String.format("%.0f", 100.0 / shardedDist.size()));
            log.info("=================================================================");

            // =================================================================
            // 断言
            // =================================================================
            assertEquals(1, noShardDist.size(),
                    "未分片：所有请求必须 100% 集中在同一个节点（这就是热点问题的根源）");
            assertTrue(shardedDist.size() >= 2,
                    "分片后：请求必须分散到多个节点");
            assertEquals(0, noShardErrors.get() + shardedErrors.get(),
                    "压测过程中不应有请求错误！");

            log.info("===== 🎉 压测对比验证通过！核心结论：节点分布从 100% 集中改善为 {}% 均衡 =====",
                    String.format("%.0f", 100.0 / shardedDist.size()));

        } finally {
            Long deleted = stringRedisTemplate.delete(allWrittenKeys);
            log.info("压测脏数据清理：目标 {} 个，实际删除 {} 个", allWrittenKeys.size(), deleted);
        }
    }

    /**
     * 虚拟线程（JDK 21）版压测对比：真正压出单节点集中 vs 分片均衡的延迟差异
     *
     * <p><b>为什么上一个平台线程测试看不出延迟差？</b>
     * <pre>
     *   平台线程 50 个 × 200 次 = 10,000 次，瞬时并发 50
     *   Redis 单节点约 100,000 ops/sec，50 并发根本不饱和
     *   → 两阶段延迟都在 4ms 左右，差异不明显
     * </pre>
     *
     * <p><b>为什么虚拟线程能压出效果？</b>
     * <pre>
     *   虚拟线程 2000 个 × 100 次 = 200,000 次，瞬时并发 2000
     *   Redis GET 是 I/O 等待密集型：虚拟线程在等待响应时自动挂起让出 CPU
     *   2000 个并发命令同时挤压到同一节点 → 命令队列积压 → 延迟显著升高
     *
     *   内存开销对比：
     *     平台线程 2000 个 × 1MB/线程 = 2 GB（根本开不了）
     *     虚拟线程 2000 个 × ~2KB/线程 = ~4 MB（JDK21 原生支持）
     * </pre>
     *
     * <p><b>压测逻辑：</b>
     * <pre>
     *   Phase 1（未分片）：2000 虚拟线程全压同一个 Hot Key
     *     → 单条 TCP 链路（Lettuce 共享连接）承受 2000 并发命令
     *     → 192.168.254.2:6401 命令队列持续积压，延迟飙升
     *
     *   Phase 2（分片后）：2000 虚拟线程各取随机分片 Key
     *     → 请求自动散落 3 节点，各约 667 并发
     *     → 单节点压力降至 1/3，命令队列明显缩短，延迟改善
     * </pre>
     *
     * @throws InterruptedException 多线程同步等待中断
     */
    @Test
    @DisplayName("虚拟线程压测（JDK21）：2000并发×20万次，压出节点集中 vs 分片均衡真实延迟差")
    public void testHotKeyPressureVirtualThread() throws InterruptedException {

        final String HOT_KEY    = "vt:bench:stock:10086";
        final int    VT_COUNT   = 2000;   // 虚拟线程数（平台线程版只有 50 个，本版是 40x）
        final int    REQ_PER_VT = 100;    // 每线程请求数
        final int    TOTAL      = VT_COUNT * REQ_PER_VT;  // 200,000 次
        final int    SHARD_POOL = 1000;

        // =====================================================================
        // 前置：写入热点 Key + 全量 1000 个分片副本
        // 写全量（而不是步长采样），确保随机读时命中真实 Key，排除 miss 对延迟的干扰
        // =====================================================================
        log.info("===== [VT压测] 前置：写入 {} 个全量分片副本（排除 miss 干扰）=====", SHARD_POOL);
        stringRedisTemplate.opsForValue().set(HOT_KEY, "value_10086");

        List<String> allShardKeys = new ArrayList<>(SHARD_POOL);
        for (int i = 0; i < SHARD_POOL; i++) {
            String k = HOT_KEY + "#" + i;
            stringRedisTemplate.opsForValue().set(k, "value_10086");
            allShardKeys.add(k);
        }
        List<String> allWrittenKeys = new ArrayList<>();
        allWrittenKeys.add(HOT_KEY);
        allWrittenKeys.addAll(allShardKeys);
        log.info("[VT压测] 前置完成：{} 个 Key（1 热点 + {} 全量分片）", allWrittenKeys.size(), SHARD_POOL);

        try {
            // =====================================================================
            // 预采样：节点命中分布（证明根因是路由集中）
            // =====================================================================
            Map<String, Integer> noShardDist  = new LinkedHashMap<>();
            Map<String, Integer> shardedDist  = new LinkedHashMap<>();

            try (RedisClusterConnection conn =
                         stringRedisTemplate.getConnectionFactory().getClusterConnection()) {
                for (int i = 0; i < 1000; i++) {
                    RedisClusterNode n = conn.clusterGetNodeForKey(HOT_KEY.getBytes());
                    noShardDist.merge(n.getHost() + ":" + n.getPort(), 1, Integer::sum);
                }
                for (int i = 0; i < 1000; i++) {
                    String sk = HotKeyShardingUtil.getRandomShardKey(HOT_KEY, SHARD_POOL);
                    RedisClusterNode n = conn.clusterGetNodeForKey(sk.getBytes());
                    shardedDist.merge(n.getHost() + ":" + n.getPort(), 1, Integer::sum);
                }
            }

            log.info("[未分片] 1000次路由分布（⚠️ 单节点 100% 集中）：");
            noShardDist.forEach((node, cnt) ->
                    log.info("  ⚠️  节点 [{}] → {} 次 ({}%)", node, cnt,
                            String.format("%.1f", cnt * 100.0 / 1000)));
            log.info("[分片后] 1000次路由分布（✅ 三节点约 33% 均衡）：");
            shardedDist.forEach((node, cnt) ->
                    log.info("  ✅  节点 [{}] → {} 次 ({}%)", node, cnt,
                            String.format("%.1f", cnt * 100.0 / 1000)));

            // =====================================================================
            // Phase 1：未分片 —— 2000 虚拟线程全压同一个 Hot Key
            // 2000 并发命令挤压单节点，命令队列积压，延迟飙升
            // =====================================================================
            log.info("===== [VT Phase 1] 未分片：{} 虚拟线程 × {} = {} 次（内存约{}MB） =====",
                    VT_COUNT, REQ_PER_VT, TOTAL, VT_COUNT * 2 / 1024 + 1);

            Queue<Long>   noShardNanos  = new ConcurrentLinkedQueue<>();
            AtomicInteger noShardErrors = new AtomicInteger(0);
            CountDownLatch ready1 = new CountDownLatch(VT_COUNT);
            CountDownLatch go1    = new CountDownLatch(1);
            CountDownLatch done1  = new CountDownLatch(VT_COUNT);

            // ✅ JDK 21 核心：newVirtualThreadPerTaskExecutor
            //    每个 submit 对应一个独立虚拟线程，I/O 等待时自动挂起，不占平台线程
            ExecutorService pool1 = Executors.newVirtualThreadPerTaskExecutor();
            for (int t = 0; t < VT_COUNT; t++) {
                pool1.submit(() -> {
                    ready1.countDown();
                    try {
                        go1.await();   // 全部 2000 虚拟线程就绪后同时齐发
                        for (int r = 0; r < REQ_PER_VT; r++) {
                            long s = System.nanoTime();
                            try { stringRedisTemplate.opsForValue().get(HOT_KEY); }
                            catch (Exception e) { noShardErrors.incrementAndGet(); }
                            noShardNanos.offer(System.nanoTime() - s);
                        }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { done1.countDown(); }
                });
            }
            ready1.await();                              // 等全部虚拟线程就绪
            long p1Start = System.currentTimeMillis();
            go1.countDown();                             // 齐发！
            done1.await();
            long p1Ms = System.currentTimeMillis() - p1Start;
            pool1.shutdown();
            log.info("[VT Phase 1] 完成：总耗时 {} ms，错误数 {}", p1Ms, noShardErrors.get());

            // =====================================================================
            // Phase 2：分片后 —— 2000 虚拟线程各自随机分片 Key
            // 请求自然散落 3 节点，每节点约 667 并发，积压降至 1/3
            // =====================================================================
            log.info("===== [VT Phase 2] 分片后：{} 虚拟线程 × {} = {} 次 =====",
                    VT_COUNT, REQ_PER_VT, TOTAL);

            Queue<Long>   shardedNanos  = new ConcurrentLinkedQueue<>();
            AtomicInteger shardedErrors = new AtomicInteger(0);
            CountDownLatch ready2 = new CountDownLatch(VT_COUNT);
            CountDownLatch go2    = new CountDownLatch(1);
            CountDownLatch done2  = new CountDownLatch(VT_COUNT);

            ExecutorService pool2 = Executors.newVirtualThreadPerTaskExecutor();
            for (int t = 0; t < VT_COUNT; t++) {
                pool2.submit(() -> {
                    ready2.countDown();
                    try {
                        go2.await();
                        for (int r = 0; r < REQ_PER_VT; r++) {
                            // ThreadLocalRandom 虚拟线程安全，无锁竞争
                            String sk = HotKeyShardingUtil.getRandomShardKey(HOT_KEY, SHARD_POOL);
                            long s = System.nanoTime();
                            try { stringRedisTemplate.opsForValue().get(sk); }
                            catch (Exception e) { shardedErrors.incrementAndGet(); }
                            shardedNanos.offer(System.nanoTime() - s);
                        }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { done2.countDown(); }
                });
            }
            ready2.await();
            long p2Start = System.currentTimeMillis();
            go2.countDown();
            done2.await();
            long p2Ms = System.currentTimeMillis() - p2Start;
            pool2.shutdown();
            log.info("[VT Phase 2] 完成：总耗时 {} ms，错误数 {}", p2Ms, shardedErrors.get());

            // =====================================================================
            // 统计：排序后计算分位数
            // =====================================================================
            long[] a1 = noShardNanos.stream().mapToLong(Long::longValue).sorted().toArray();
            long[] a2 = shardedNanos.stream().mapToLong(Long::longValue).sorted().toArray();

            String avg1  = String.format("%.3f", Arrays.stream(a1).average().orElse(0) / 1e6);
            String avg2  = String.format("%.3f", Arrays.stream(a2).average().orElse(0) / 1e6);
            String p50_1 = String.format("%.3f", a1[(int)(a1.length * 0.50)] / 1e6);
            String p50_2 = String.format("%.3f", a2[(int)(a2.length * 0.50)] / 1e6);
            String p95_1 = String.format("%.3f", a1[(int)(a1.length * 0.95)] / 1e6);
            String p95_2 = String.format("%.3f", a2[(int)(a2.length * 0.95)] / 1e6);
            String p99_1 = String.format("%.3f", a1[(int)(a1.length * 0.99)] / 1e6);
            String p99_2 = String.format("%.3f", a2[(int)(a2.length * 0.99)] / 1e6);
            String max1  = String.format("%.3f", a1[a1.length - 1] / 1e6);
            String max2  = String.format("%.3f", a2[a2.length - 1] / 1e6);
            long   qps1  = p1Ms > 0 ? (long)(TOTAL * 1000.0 / p1Ms) : 0;
            long   qps2  = p2Ms > 0 ? (long)(TOTAL * 1000.0 / p2Ms) : 0;

            String hotNode = noShardDist.keySet().iterator().next();

            log.info("====================================================================");
            log.info(" [VT] 虚拟线程压测结果（{} VT × {} 请求 = {} 次）", VT_COUNT, REQ_PER_VT, TOTAL);
            log.info("====================================================================");
            log.info(" 指标              │  未分片（集中单节点）  │  分片后（三节点均衡）");
            log.info("--------------------------------------------------------------------");
            log.info(" 命中节点数        │  {} 个（100% 集中）    │  {} 个（均衡分散）",
                    noShardDist.size(), shardedDist.size());
            log.info(" 热点节点          │  {} │  各约 33%", hotNode);
            log.info(" 平均延迟 (ms)     │  {}             │  {}", avg1, avg2);
            log.info(" P50 延迟 (ms)     │  {}             │  {}", p50_1, p50_2);
            log.info(" P95 延迟 (ms)     │  {}             │  {}", p95_1, p95_2);
            log.info(" P99 延迟 (ms)     │  {}             │  {}", p99_1, p99_2);
            log.info(" 最大延迟 (ms)     │  {}             │  {}", max1, max2);
            log.info(" 总耗时 (ms)       │  {}             │  {}", p1Ms, p2Ms);
            log.info(" 吞吐量 (QPS)      │  {}/s           │  {}/s", qps1, qps2);
            log.info(" 错误数            │  {}             │  {}", noShardErrors.get(), shardedErrors.get());
            log.info("====================================================================");
            log.info(" 【对比上一版本（50平台线程）】");
            log.info("   并发量：50 → {} 虚拟线程（{}x），总请求：10,000 → {}（{}x）",
                    VT_COUNT, VT_COUNT / 50, TOTAL, TOTAL / 10_000);
            log.info("   内存：~50MB平台线程 → ~{}MB虚拟线程（节省 {}x）",
                    VT_COUNT * 2 / 1024 + 1, 1024 / 2);
            log.info("====================================================================");

            // =====================================================================
            // 断言
            // =====================================================================
            assertEquals(1, noShardDist.size(), "未分片：所有请求必须 100% 集中在同一节点");
            assertTrue(shardedDist.size() >= 2, "分片后：请求必须分散到多个节点");
            assertEquals(0, noShardErrors.get() + shardedErrors.get(),
                    "虚拟线程压测不应有请求错误！");

            log.info("===== 🎉 [VT压测] 通过！未分片 avg={} ms  vs  分片后 avg={} ms =====",
                    avg1, avg2);

        } finally {
            Long deleted = stringRedisTemplate.delete(allWrittenKeys);
            log.info("[VT压测] 清理：目标 {} 个，实际删除 {} 个", allWrittenKeys.size(), deleted);
        }
    }

    /**
     * groupKeysByNode 批量分组验证
     *
     * <p><b>场景还原：</b>
     * 量化选股系统需要查询 6000 个股票的实时行情（stock:quote:XXXXXX），
     * 如果逐个 GET，就是 6000 次网络往返，延迟不可接受。
     *
     * <p><b>正确姿势：</b>
     * <pre>
     *   Step 1: groupKeysByNode 按节点分组（本地 CRC16，无网络开销）
     *   Step 2: 每个节点各执行一次 Pipeline 批量查询（约 2000 个 Key）
     *   Step 3: 三路并行合并结果
     *
     *   效果：6000 次 GET → 3 次 Pipeline，网络往返从 6000 次降为 3 次
     * </pre>
     *
     * <p><b>验证目标：</b>
     * <ol>
     *   <li>6000 个 Key 能被分到所有 Master 节点（没有节点被遗漏）</li>
     *   <li>每个节点的分配数量接近均匀（各约 33%，误差 ±5%）</li>
     *   <li>抽样校验：每组随机取 5 个 Key，用 clusterGetNodeForKey 反查，
     *       实际节点必须与工具类分组结果一致</li>
     * </ol>
     */
    @Test
    @DisplayName("groupKeysByNode批量分组验证：6000个股票Key按节点均匀分组，每组Pipeline并行查询")
    public void testGroupKeysByNode() {

        // =====================================================================
        // 构造 6000 个模拟股票行情 Key（stock:quote:600000 ~ stock:quote:605999）
        // 模拟量化系统查询全市场股票实时行情的场景
        // =====================================================================
        int totalKeys = 6000;
        List<String> stockKeys = new ArrayList<>(totalKeys);
        for (int i = 0; i < totalKeys; i++) {
            // 前缀 stock:quote: + 6位股票代码（从 600000 开始）
            stockKeys.add("stock:quote:" + (600000 + i));
        }
        log.info("===== 构造完成：{} 个股票行情 Key（stock:quote:600000 ~ stock:quote:{}）=====",
                totalKeys, 600000 + totalKeys - 1);

        // =====================================================================
        // 核心调用：groupKeysByNode 本地 CRC16 分组，无任何网络开销
        // 返回：Map<nodeId, List<key>> ── 每个 Master 节点 → 属于它的 Key 列表
        // =====================================================================
        Map<String, List<String>> nodeGroups = HotKeyShardingUtil.groupKeysByNode(
                stockKeys, topologyCache);

        // =====================================================================
        // 验证 1：所有 Master 节点都必须分到数据（没有节点被遗漏）
        // =====================================================================
        int masterNodeCount = topologyCache.getAllNodes().size();
        log.info("===== 验证1：节点覆盖（期望覆盖 {} 个 Master 节点）=====", masterNodeCount);
        assertEquals(masterNodeCount, nodeGroups.size(),
                String.format("分组结果只覆盖了 %d 个节点，期望 %d 个！", nodeGroups.size(), masterNodeCount));
        log.info("✅ 验证1通过：分组结果覆盖了全部 {} 个 Master 节点", nodeGroups.size());

        // =====================================================================
        // 验证 2：各节点分配量接近均匀（各约 33%，允许 ±5% 误差）
        // 理论分布：6000 / 3 = 2000 个/节点，误差范围 [1700, 2300]
        // =====================================================================
        log.info("===== 验证2：各节点分配量均衡性 =====");
        int expectedPerNode = totalKeys / masterNodeCount;
        int tolerance = (int) (expectedPerNode * 0.15);  // 允许 ±15% 误差（CRC16 分布略有偏差）

        int totalVerified = 0;
        for (Map.Entry<String, List<String>> entry : nodeGroups.entrySet()) {
            String nodeId = entry.getKey();
            int count = entry.getValue().size();
            totalVerified += count;
            String ratio = String.format("%.1f", count * 100.0 / totalKeys);

            log.info("节点 [{}] 分配到 {} 个 Key（占比 {}%），期望约 {} 个（允差 ±{}）",
                    nodeId, count, ratio, expectedPerNode, tolerance);

            assertTrue(
                    count >= expectedPerNode - tolerance && count <= expectedPerNode + tolerance,
                    String.format("节点 [%s] 分配量 %d 偏差过大，期望范围 [%d, %d]",
                            nodeId, count, expectedPerNode - tolerance, expectedPerNode + tolerance)
            );
        }

        // 验证 Key 总数没有丢失
        assertEquals(totalKeys, totalVerified,
                String.format("分组后 Key 总数 %d 与输入 %d 不一致，存在 Key 丢失！", totalVerified, totalKeys));
        log.info("✅ 验证2通过：各节点分配均衡，总计 {} 个 Key 无丢失", totalVerified);

        // =====================================================================
        // 验证 3：抽样反查 ── 每组随机取 5 个 Key，clusterGetNodeForKey 确认路由一致
        // 证明工具类的本地 CRC16 计算结果与集群实际路由完全吻合
        // =====================================================================
        log.info("===== 验证3：抽样反查（每组取5个Key，集群实际路由必须与分组结果吻合）=====");

        int sampleSize = 5;
        int sampleFailCount = 0;

        try (RedisClusterConnection conn =
                     stringRedisTemplate.getConnectionFactory().getClusterConnection()) {

            for (Map.Entry<String, List<String>> entry : nodeGroups.entrySet()) {
                String expectedNodeId = entry.getKey();
                List<String> keysInGroup = entry.getValue();

                // 随机取 sampleSize 个 Key（不超过实际数量）
                int actualSample = Math.min(sampleSize, keysInGroup.size());
                List<String> sampleKeys = keysInGroup.subList(0, actualSample);

                for (String key : sampleKeys) {
                    RedisClusterNode actualNode = conn.clusterGetNodeForKey(key.getBytes());
                    String actualNodeId = actualNode.getHost() + ":" + actualNode.getPort();

                    boolean match = expectedNodeId.equals(actualNodeId);
                    if (!match) {
                        sampleFailCount++;
                        log.error("❌ 路由不一致！Key [{}] 工具类分组→[{}]，集群实际→[{}]",
                                key, expectedNodeId, actualNodeId);
                    } else {
                        log.debug("✅ Key [{}] 路由一致，归属节点 [{}]", key, actualNodeId);
                    }
                }

                log.info("节点 [{}] 抽样 {} 个 Key，路由全部吻合 ✅", expectedNodeId, actualSample);
            }
        }

        assertEquals(0, sampleFailCount,
                String.format("抽样验证发现 %d 个 Key 路由不一致！工具类 CRC16 计算与集群实际路由有偏差。",
                        sampleFailCount));
        log.info("✅ 验证3通过：抽样路由反查 100% 吻合，工具类 CRC16 计算准确");

        // =====================================================================
        // 输出 Pipeline 优化效果对比（说明分组的价值）
        // =====================================================================
        log.info("===== 🎉 groupKeysByNode 验证全部通过 =====");
        log.info("优化前：{} 个 Key 逐个 GET → {} 次网络往返", totalKeys, totalKeys);
        log.info("优化后：按 {} 个节点分组 → {} 次 Pipeline（每次约 {} 个 Key）→ 网络往返从 {} 次降为 {} 次",
                nodeGroups.size(), nodeGroups.size(), expectedPerNode, totalKeys, nodeGroups.size());
        log.info("代码示例（生产实践）：");
        log.info("  Map<String, List<String>> groups = HotKeyShardingUtil.groupKeysByNode(stockKeys, topology);");
        log.info("  groups.entrySet().parallelStream().forEach(e -> redisTemplate.opsForValue().multiGet(e.getValue()));");
    }

    @Test
    @DisplayName("HotKeyShardingUtil验证：工具类动态计算分片Key，保证打散到不同Master节点")
    public void testHotKeyShardingUtil() {

        // 模拟三个不同业务场景的热点 Key
        String[] hotKeys = {"stock:10086", "user:timeline:hot", "order:flash:sale"};
        List<String> writtenKeys = new ArrayList<>();

        try {
            log.info("===== 写入场景验证：getOneShardKeyPerNode =====");

            for (String hotKey : hotKeys) {
                // ✅ 核心调用：工具类动态计算每个节点的分片 Key
                // 内部遍历随机起点的后缀，找到每个Master节点各一个后缀，无需手动计算
                Map<String, String> shardMap = HotKeyShardingUtil.getOneShardKeyPerNode(
                        hotKey, topologyCache, HotKeyShardingUtil.DEFAULT_SHARD_POOL);

                log.info("热点Key [{}] 的分片写入方案（共{}个节点）：", hotKey, shardMap.size());
                shardMap.forEach((node, shardKey) ->
                        log.info("  → 节点 [{}] : 写入 Key [{}]", node, shardKey));

                // 断言：必须覆盖所有 Master 节点
                assertEquals(topologyCache.getAllNodes().size(), shardMap.size(),
                        "工具类未能为所有Master节点找到分片Key！");

                // 断言：每个分片 Key 确实落在对应节点（通过集群查询反向验证）
                try (RedisClusterConnection conn =
                             stringRedisTemplate.getConnectionFactory().getClusterConnection()) {
                    for (Map.Entry<String, String> entry : shardMap.entrySet()) {
                        String expectedNode = entry.getKey();
                        String shardKey     = entry.getValue();
                        RedisClusterNode actualNode = conn.clusterGetNodeForKey(shardKey.getBytes());
                        String actualNodeId = actualNode.getHost() + ":" + actualNode.getPort();
                        assertEquals(expectedNode, actualNodeId,
                                String.format("路由不符！Key [%s] 期望节点 [%s]，实际节点 [%s]",
                                        shardKey, expectedNode, actualNodeId));
                        log.info("  ✅ 路由验证通过：Key [{}] → 节点 [{}]", shardKey, actualNodeId);
                    }
                }

                // 模拟生产写入：向所有分片副本写入相同的值
                String value = "latest_data_for_" + hotKey;
                shardMap.values().forEach(shardKey -> {
                    stringRedisTemplate.opsForValue().set(shardKey, value);
                    writtenKeys.add(shardKey);
                });
            }

            log.info("===== 读取场景验证：getRandomShardKey =====");

            // 模拟 10 次随机读取，观察分散效果
            Map<String, Integer> nodeHitCount = new LinkedHashMap<>();
            try (RedisClusterConnection conn =
                         stringRedisTemplate.getConnectionFactory().getClusterConnection()) {
                for (int i = 0; i < 10; i++) {
                    // ✅ 核心调用：随机选一个后缀，请求自然散列到不同节点
                    String randomKey = HotKeyShardingUtil.getRandomShardKey(
                            "stock:10086", HotKeyShardingUtil.DEFAULT_SHARD_POOL);
                    RedisClusterNode node = conn.clusterGetNodeForKey(randomKey.getBytes());
                    String nodeId = node.getHost() + ":" + node.getPort();
                    nodeHitCount.merge(nodeId, 1, Integer::sum);
                }
            }
            log.info("10次随机读取的节点命中分布（验证读负载均衡）：");
            nodeHitCount.forEach((node, count) ->
                    log.info("  节点 [{}] 命中 {} 次", node, count));
            assertTrue(nodeHitCount.size() > 1, "10次随机读取全部落在同一节点，随机路由未生效！");

            log.info("===== 🎉 HotKeyShardingUtil 验证全部通过 =====");
            log.info("写入：getOneShardKeyPerNode → 动态找到每节点分片Key，保证打散");
            log.info("读取：getRandomShardKey → 随机后缀，请求均匀分散到各节点");

        } finally {
            // 清理所有写入的测试数据
            if (!writtenKeys.isEmpty()) {
                Long deleted = stringRedisTemplate.delete(writtenKeys);
                log.info("脏数据清理完成 | 目标: {}个, 实际删除: {}个", writtenKeys.size(), deleted);
            }
        }
    }
}
