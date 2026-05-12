package com.hao.redis.common.util;

import com.hao.redis.integration.cluster.RedisClusterTopologyCache;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 热点 Key 分片工具类 (Hot Key Sharding Utility)
 *
 * <p><b>解决的问题：</b>
 * Redis Cluster 用 {@code CRC16(key) % 16384} 路由，同一个 Key 永远打同一个 Master 节点。
 * 热点 Key（如秒杀库存、热门行情）高并发时导致单节点 CPU 100%，其他节点空闲。
 *
 * <p><b>核心方案：</b>
 * 将热点 Key 复制为 N 个分片副本，后缀不同，CRC16 自然散列到不同节点：
 * <pre>
 *   stock:10086#0  → Master-A（CRC16 决定）
 *   stock:10086#47 → Master-B
 *   stock:10086#91 → Master-C
 * </pre>
 *
 * <p><b>两种使用场景：</b>
 * <ol>
 *   <li><b>写入</b>：调用 {@link #getOneShardKeyPerNode}，拿到「每个Master节点各一个分片Key」，
 *       同步更新所有分片副本，保证各节点数据一致</li>
 *   <li><b>读取</b>：调用 {@link #getRandomShardKey}，随机选一个后缀，
 *       不同请求落到不同节点，天然均衡读负载</li>
 * </ol>
 *
 * <p><b>关键设计决策——为什么后缀不用 Hash Tag ({})：</b>
 * <pre>
 *   Hash Tag 设计目的是「强制多 Key 落同一节点」（用于跨 Key 事务/MGET）
 *   分片目标是「打散到不同节点」，与 Hash Tag 目的完全相反
 *   所以分片后缀不加花括号，让 CRC16 计算完整字符串，自然散列
 * </pre>
 *
 * @author hli
 * @see RedisSlotUtil    CRC16 Slot 计算
 * @see RedisClusterTopologyCache  集群拓扑缓存（Slot→Node 映射）
 */
@Slf4j
public class HotKeyShardingUtil {

    /** 分片后缀分隔符，不使用 {} 避免触发 Redis Hash Tag 机制 */
    private static final String SHARD_SEPARATOR = "#";

    /** 默认分片池大小：后缀范围 0~999，3节点集群下每节点约承担 333 个分片 */
    public static final int DEFAULT_SHARD_POOL = 1000;

    private HotKeyShardingUtil() {
        // 工具类禁止实例化
    }

    /**
     * 【写入专用】为热点 Key 找到每个 Master 节点各一个分片 Key
     *
     * <p><b>算法：</b>
     * <pre>
     *   1. 从随机后缀起点出发（避免固定后缀形成新热点）
     *   2. 对每个后缀：计算 Slot = CRC16(baseKey#suffix) % 16384
     *                          Node = topologyCache.getNodeBySlot(slot)
     *   3. 若该 Node 还没有分片 Key → 记录
     *   4. 直到所有 Master 节点都找到对应的分片 Key → 提前退出
     * </pre>
     *
     * <p><b>性能说明：</b>
     * CRC16 是纯本地计算，无网络开销。
     * 平均只需遍历约 {@code shardPool / nodeCount * nodeCount} 个后缀即可覆盖全部节点，
     * 3节点时通常 10~50 次迭代即可完成，耗时微秒级。
     *
     * <p><b>调用示例（写入场景）：</b>
     * <pre>
     *   Map&lt;String, String&gt; shardKeys =
     *       HotKeyShardingUtil.getOneShardKeyPerNode("stock:10086", topologyCache, 1000);
     *   // 结果：{"192.168.254.2:6401" -> "stock:10086#47",
     *   //        "192.168.254.2:6402" -> "stock:10086#3",
     *   //        "192.168.254.3:6401" -> "stock:10086#891"}
     *
     *   // 同步写入所有分片副本（保证各节点数据一致）
     *   shardKeys.values().forEach(key -> redisClient.set(key, latestValue, ttl));
     * </pre>
     *
     * @param baseKey   原始热点 Key，如 "stock:10086"
     * @param topology  集群拓扑缓存（Slot→Node 映射，需提前加载）
     * @param shardPool 分片池大小，后缀范围 [0, shardPool)，建议 1000
     * @return Map&lt;nodeId, shardKey&gt;，每个 Master 节点对应一个分片 Key
     *         若拓扑缓存为空，返回空 Map（调用方需处理降级）
     */
    public static Map<String, String> getOneShardKeyPerNode(
            String baseKey,
            RedisClusterTopologyCache topology,
            int shardPool) {

        // 获取当前集群所有 Master 节点，用于判断何时覆盖完毕
        Set<String> allNodes = topology.getAllNodes();
        if (allNodes.isEmpty()) {
            log.error("[分片工具] 集群拓扑为空，无法计算分片路由|HotKeyShard_topology_empty,baseKey={}", baseKey);
            return new LinkedHashMap<>();
        }

        Map<String, String> nodeToShardKey = new LinkedHashMap<>();

        // 随机起点：每次调用从不同后缀开始搜索
        // 避免每次都返回相同的"代表后缀"，防止写入时形成新的固定热点
        int start = ThreadLocalRandom.current().nextInt(shardPool);

        for (int i = 0; i < shardPool; i++) {
            // 循环遍历：从 start 开始，到 start + shardPool - 1，对 shardPool 取模
            int suffix = (start + i) % shardPool;
            String shardKey = baseKey + SHARD_SEPARATOR + suffix;

            // 本地 CRC16 计算，无网络开销
            int slot = RedisSlotUtil.getSlot(shardKey);
            String node = topology.getNodeBySlot(slot);

            // 该节点还没有分片 Key → 记录（每个节点只取第一个找到的）
            if (node != null && !nodeToShardKey.containsKey(node)) {
                nodeToShardKey.put(node, shardKey);
                log.debug("[分片工具] 找到节点分片|HotKeyShard_found,node={},shardKey={},slot={}", node, shardKey, slot);
            }

            // 提前退出：已为所有 Master 节点找到分片 Key
            if (nodeToShardKey.size() == allNodes.size()) {
                log.info("[分片工具] 分片Key计算完成|HotKeyShard_done,baseKey={},iterations={},nodeCount={}",
                        baseKey, i + 1, nodeToShardKey.size());
                break;
            }
        }

        if (nodeToShardKey.size() < allNodes.size()) {
            // 正常情况下不会触发（shardPool=1000 覆盖 3 节点绝对够用）
            log.warn("[分片工具] 未能覆盖全部节点|HotKeyShard_incomplete," +
                            "baseKey={},found={},total={}", baseKey, nodeToShardKey.size(), allNodes.size());
        }

        return nodeToShardKey;
    }

    /**
     * 【读取专用】随机选一个分片 Key（均衡读负载）
     *
     * <p>读取时不需要保证覆盖所有节点，只需随机分散即可。
     * 大量并发请求各自随机取后缀，统计上均匀分布到所有 Master 节点。
     *
     * <p><b>调用示例（读取场景）：</b>
     * <pre>
     *   String shardKey = HotKeyShardingUtil.getRandomShardKey("stock:10086", 1000);
     *   // 可能返回 "stock:10086#237"（随机），路由到某个 Master 节点
     *   String value = redisClient.get(shardKey);
     * </pre>
     *
     * @param baseKey    原始热点 Key
     * @param shardPool  分片池大小，与写入时保持一致
     * @return 带随机后缀的分片 Key
     */
    public static String getRandomShardKey(String baseKey, int shardPool) {
        int suffix = ThreadLocalRandom.current().nextInt(shardPool);
        String shardKey = baseKey + SHARD_SEPARATOR + suffix;
        log.debug("[分片工具] 随机读取分片Key|HotKeyShard_read,shardKey={}", shardKey);
        return shardKey;
    }

    /**
     * 【批量读取】获取覆盖所有节点的分片 Key 列表（用于需要读全量副本的场景）
     *
     * <p>适用场景：需要从所有分片副本聚合数据时（如统计总库存 = 所有副本库存之和）。
     * 普通读取场景请用 {@link #getRandomShardKey}。
     *
     * @param baseKey  原始热点 Key
     * @param topology 集群拓扑缓存
     * @param shardPool 分片池大小
     * @return 每个节点的代表分片 Key 集合
     */
    public static Collection<String> getAllNodeShardKeys(
            String baseKey,
            RedisClusterTopologyCache topology,
            int shardPool) {
        return getOneShardKeyPerNode(baseKey, topology, shardPool).values();
    }

    // ==================== 场景二：批量Key按节点分组（解决批量查询效率问题）====================

    /**
     * 【批量查询专用】将一批 Key 按所在 Master 节点分组
     *
     * <p><b>解决的问题：</b>
     * 查询 6000 个股票行情时，如果逐个 GET，就是 6000 次网络往返。
     * 先按节点分组，再对每个节点做 Pipeline 批量读，变成 3 次 Pipeline，
     * 每次批量取约 2000 个，网络往返从 6000 次降为 3 次。
     *
     * <p><b>使用场景：</b>
     * <pre>
     *   // Step 1: 构造 6000 个股票 Key
     *   List&lt;String&gt; stockKeys = stockCodes.stream()
     *           .map(code -> "stock:quote:" + code)
     *           .collect(toList());
     *
     *   // Step 2: 按节点分组（纯本地计算，无网络开销）
     *   Map&lt;String, List&lt;String&gt;&gt; nodeGroups =
     *           HotKeyShardingUtil.groupKeysByNode(stockKeys, topology);
     *   // 结果示例：
     *   // {"192.168.254.2:6401" → ["stock:quote:600519", "stock:quote:601318", ...] (约2000个)}
     *   // {"192.168.254.2:6402" → ["stock:quote:000858", "stock:quote:000002", ...] (约2000个)}
     *   // {"192.168.254.3:6401" → ["stock:quote:601166", "stock:quote:002415", ...] (约2000个)}
     *
     *   // Step 3: 对每个节点并行 Pipeline 批量查询，结果汇总
     *   Map&lt;String, String&gt; allResults = new ConcurrentHashMap&lt;&gt;();
     *   nodeGroups.entrySet().parallelStream().forEach(entry -&gt; {
     *       List&lt;String&gt; keys = entry.getValue();
     *       List&lt;String&gt; values = redisTemplate.opsForValue().multiGet(keys); // Pipeline
     *       for (int i = 0; i &lt; keys.size(); i++) {
     *           if (values.get(i) != null) allResults.put(keys.get(i), values.get(i));
     *       }
     *   });
     * </pre>
     *
     * <p><b>注意：</b>Redis Cluster 的 Pipeline 只能对同一节点的 Key 生效。
     * 先用本方法分组，再对每组 Pipeline，是集群下批量查询的正确姿势。
     * 若不分组直接对 6000 个跨节点 Key 做 Pipeline，Spring Data Redis 会自动拆分，
     * 但内部还是多次网络往返，不如显式分组后并行更可控。
     *
     * @param keys     要分组的 Key 集合（如 6000 个股票行情 Key）
     * @param topology 集群拓扑缓存（提供 Slot→Node 映射）
     * @return Map&lt;nodeId, List&lt;key&gt;&gt; 按节点分组后的 Key 列表，顺序与输入一致
     */
    public static Map<String, List<String>> groupKeysByNode(
            Collection<String> keys,
            RedisClusterTopologyCache topology) {

        Map<String, List<String>> nodeToKeys = new LinkedHashMap<>();

        for (String key : keys) {
            // 本地 CRC16 计算，无网络开销
            int slot = RedisSlotUtil.getSlot(key);
            String node = topology.getNodeBySlot(slot);

            if (node == null) {
                log.warn("[分片工具] Key [{}] 未找到对应节点，跳过|HotKeyShard_node_not_found,key={}", key, key);
                continue;
            }
            nodeToKeys.computeIfAbsent(node, k -> new ArrayList<>()).add(key);
        }

        log.info("[分片工具] Key分组完成|HotKeyShard_group_done," +
                        "totalKeys={},nodeCount={},distribution={}",
                keys.size(),
                nodeToKeys.size(),
                nodeToKeys.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().size() + "个")
                        .reduce((a, b) -> a + ", " + b).orElse(""));

        return nodeToKeys;
    }
}
