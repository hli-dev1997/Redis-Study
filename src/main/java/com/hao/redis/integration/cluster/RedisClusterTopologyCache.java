package com.hao.redis.integration.cluster;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Redis 集群拓扑缓存
 *
 * 类职责：
 * 1. 缓存 Redis 集群的 Slot -> Node 映射关系。
 * 2. 定时刷新拓扑，以适应集群扩缩容。
 *
 * 设计目的：
 * - 为客户端路由提供实时的、高性能的拓扑查询。
 * - 避免每次计算路由都去查询 Redis，降低延迟。
 *
 * 核心实现思路：
 * - 启动时执行一次全量拉取。
 * - 使用 `TreeMap` 或 `ConcurrentSkipListMap` 存储 Slot 范围，利用其 `floorEntry` 方法快速定位 Slot 所属的节点。
 * - 开启定时任务，周期性地与 Redis 同步拓扑信息。
 */
@Slf4j
@Component
public class RedisClusterTopologyCache implements CommandLineRunner {

    @Autowired
    private RedisConnectionFactory connectionFactory;

    /**
     * 缓存 Slot -> Node 的映射
     * Key: Slot 的起始范围
     * Value: 节点信息 (IP:Port)
     *
     * 使用 ConcurrentSkipListMap 保证线程安全，并提供高效的范围查找。
     */
    private final NavigableMap<Integer, String> slotNodeCache = new ConcurrentSkipListMap<>();

    /**
     * 项目启动时，执行第一次拓扑加载
     */
    @Override
    public void run(String... args) {
        refreshTopology();
    }

    /**
     * 定时刷新拓扑缓存
     * 每 30 秒执行一次
     */
    @Scheduled(fixedRate = 30000)
    public void refreshTopology() {
//        log.info("开始刷新Redis集群拓扑|Start_refreshing_redis_cluster_topology");
        try (RedisClusterConnection connection = connectionFactory.getClusterConnection()) {
            // 获取集群所有主节点
            Iterable<RedisClusterNode> masterNodes = connection.clusterGetNodes();
            
            // 创建一个新的临时 Map 用于更新
            NavigableMap<Integer, String> newCache = new TreeMap<>();

            for (RedisClusterNode master : masterNodes) {
                // 过滤掉从节点，只关注主节点
                if (master.isMaster()) {
                    // 获取该主节点负责的 Slot 范围
                    RedisClusterNode.SlotRange range = master.getSlotRange();
                    if (range != null && !range.getSlots().isEmpty()) {
                        // Spring Data Redis 的 SlotRange 可能包含多个不连续的区间，但通常是一个连续区间
                        // 这里我们取区间的起始值作为 Key
                        // 注意：Spring Data Redis 的 SlotRange API 比较简单，可能需要遍历 slots
                        // 为了简化，我们假设每个 Master 负责一段连续的 Slot，取最小的那个
                        int minSlot = range.getSlots().stream().min(Integer::compareTo).orElse(0);
                        
                        String nodeAddress = master.getHost() + ":" + master.getPort();
                        newCache.put(minSlot, nodeAddress);
                        
                        log.debug("加载拓扑映射|Loading_topology_map, minSlot={}, node={}", minSlot, nodeAddress);
                    }
                }
            }

            // 原子性地替换旧缓存
            if (!newCache.isEmpty()) {
                slotNodeCache.clear();
                slotNodeCache.putAll(newCache);
//                log.info("Redis集群拓扑刷新完成|Redis_cluster_topology_refreshed, masterCount={}", newCache.size());
            }
        } catch (Exception e) {
            log.error("刷新Redis集群拓扑失败|Refresh_redis_cluster_topology_fail", e);
        }
    }

    /**
     * 获取当前集群所有 Master 节点的唯一标识集合 (IP:Port)
     *
     * <p>用于 {@link com.hao.redis.common.util.HotKeyShardingUtil} 判断
     * 何时已经为所有节点找到了对应的分片 Key，避免无谓地遍历全部后缀。
     *
     * @return Master 节点标识集合，例如 {"192.168.254.2:6401", "192.168.254.2:6402", "192.168.254.3:6401"}
     */
    public Set<String> getAllNodes() {
        if (slotNodeCache.isEmpty()) {
            refreshTopology();
        }
        return new HashSet<>(slotNodeCache.values());
    }

    /**
     * 根据 Slot ID 查询其所属的节点地址
     *
     * @param slot Slot ID
     * @return 节点地址 (IP:Port)，如果未找到则返回 null
     */
    public String getNodeBySlot(int slot) {
        if (slotNodeCache.isEmpty()) {
            // 如果缓存为空，可能正在初始化，可以触发一次同步刷新
            refreshTopology();
        }
        
        // 使用 floorEntry 找到小于等于该 slot 的最大起始槽位
        // 例如：slot=5798, 缓存中有 {0 -> nodeA, 5461 -> nodeB, 10923 -> nodeC}
        // floorEntry(5798) 会返回 (5461, nodeB)
        var entry = slotNodeCache.floorEntry(slot);
        return entry != null ? entry.getValue() : null;
    }
}
