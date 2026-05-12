package com.hao.redis.config;

import com.hao.redis.integration.redis.RedisClientImpl;
import io.lettuce.core.api.StatefulConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.time.Duration;

/**
 * Redis 集群配置类
 * <p>
 * 类职责：
 * 构建 Lettuce 连接工厂、模板与自定义客户端封装。
 *
 * 设计目的：
 * 1. 统一 Redis 连接与序列化配置，避免多处重复。
 * 2. 面向高并发场景进行连接池与连接共享策略调优。
 *
 * 为什么需要该类：
 * Redis 连接参数涉及稳定性与性能，集中配置便于压测与线上运维。
 *
 * 核心实现思路：
 * - 读取 RedisProperties 组装集群与连接池配置。
 * - 显式关闭连接共享以提高并发吞吐。
 * - 启动时输出集群节点，便于健康校验。
 * <p>
 * 【性能调优 - 重点说明】
 * 1. 连接独占模式：通过 setShareNativeConnection(false) 强制 Lettuce 配合连接池，
 *    在高并发下利用多条物理连接并行处理，避免了单条物理连接在接收万级并发时的处理瓶颈。
 * 2. 连接池水位控制：结合业务 QPS 设定合理的 max-total 和 max-idle，
 *    保持热连接常驻，消除频繁建立 TCP 连接握手的开销。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class RedisConfig {

    private final RedisProperties redisProperties;

    /**
     * Redis 配置构造方法
     *
     * 实现逻辑：
     * 1. 注入 Spring Boot 的 RedisProperties。
     *
     * @param redisProperties Redis 配置属性
     */
    public RedisConfig(RedisProperties redisProperties) {
        // 实现思路：
        // 1. 保留配置对象供后续构建连接工厂使用。
        this.redisProperties = redisProperties;
    }

    /**
     * 创建并配置 Lettuce 连接工厂
     * <p>
     * 这里手动组装集群配置、连接池配置和客户端配置，并针对高并发场景进行连接共享调优。
     *
     * 实现逻辑：
     * 1. 读取并构建集群节点配置与连接池参数。
     * 2. 构建 Lettuce 客户端配置并实例化连接工厂。
     * 3. 关闭连接共享并初始化工厂。
     *
     * @return LettuceConnectionFactory 配置好的连接工厂
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        // 实现思路：
        // 1. 组装集群与连接池参数。
        // 2. 构建客户端配置并实例化连接工厂。
        // 3. 调整连接共享策略并完成初始化。
        // --- 1. 配置 Redis 集群节点信息 ---
        // 从 application.yml 读取节点列表 (192.168.254.x:6401)
        RedisClusterConfiguration config = new RedisClusterConfiguration(redisProperties.getCluster().getNodes());

        // 设置最大重定向次数 (防止集群拓扑变更时的死循环)
        if (redisProperties.getCluster().getMaxRedirects() != null) {
            config.setMaxRedirects(redisProperties.getCluster().getMaxRedirects());
        }
        // 设置集群密码
        if (StringUtils.hasText(redisProperties.getPassword())) {
            config.setPassword(redisProperties.getPassword());
        }

        // --- 2. 配置连接池参数 (GenericObjectPool) ---
        // Lettuce 使用 Commons-Pool2 来管理连接
        GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
        RedisProperties.Pool pool = redisProperties.getLettuce().getPool();
        if (pool != null) {
            // 最大连接数：压测时建议设置为 1000 或更高，配合虚拟线程使用
            poolConfig.setMaxTotal(pool.getMaxActive());
            // 最大空闲连接：保持较高的水位，避免频繁创建销毁连接
            poolConfig.setMaxIdle(pool.getMaxIdle());
            // 最小空闲连接：保留底座连接
            poolConfig.setMinIdle(pool.getMinIdle());
            // 获取连接最大等待时间：建议 3-5秒，超时则抛出异常
            poolConfig.setMaxWait(pool.getMaxWait());
        }

        // 设置默认命令超时时间 (默认5秒)
        Duration timeout = redisProperties.getTimeout() != null ? redisProperties.getTimeout() : Duration.ofSeconds(5);

        // --- 3. 构建 Lettuce 客户端配置 ---
        // 使用连接池模式构建配置
        LettuceClientConfiguration clientConfiguration = LettucePoolingClientConfiguration.builder()
                .commandTimeout(timeout)
                .poolConfig(poolConfig)
                .build();

        // --- 4. 实例化连接工厂 ---
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config, clientConfiguration);

        // 开启连接校验，确保获取到的连接是可用的
        connectionFactory.setValidateConnection(true);

        // 核心性能优化：
        // 默认值为 true（开启共享），会复用同一条物理 TCP 连接。
        // 在高并发下单连接易成瓶颈，关闭共享可提升并发吞吐。
        // 作用：配合连接池让每次操作获取独立物理连接。
        // 结果：连接数与吞吐能力成正比提升。
        // 核心代码：关闭连接共享
        connectionFactory.setShareNativeConnection(false);
        // 初始化工厂
        connectionFactory.afterPropertiesSet();
        log.info("Redis集群连接工厂创建完成|Redis_cluster_factory_created,nodes={},poolMax={},shareNativeConnection=false",
                redisProperties.getCluster().getNodes(),
                poolConfig.getMaxTotal());

        return connectionFactory;
    }

    /**
     * 配置 StringRedisTemplate
     * <p>
     * 这是一个针对 String 类型优化的模板，键和值都是 String 序列化。
     * 也是压测中最常用的模板。
     *
     * 实现逻辑：
     * 1. 构建模板并注入连接工厂。
     * 2. 初始化模板并输出日志确认。
     */
    @Bean
    public StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory connectionFactory) {
        // 实现思路：
        // 1. 注入连接工厂并完成模板初始化。
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        // 核心代码：初始化模板确保依赖生效
        template.afterPropertiesSet();
        log.info("StringRedisTemplate初始化完成|StringRedisTemplate_init_done");
        return template;
    }

    /**
     * 配置自定义的 RedisClient 封装类
     *
     * 实现逻辑：
     * 1. 使用 StringRedisTemplate 构建客户端封装。
     *
     * @param stringRedisTemplate Redis 模板
     * @return RedisClient 客户端封装
     */
    @Bean
    public com.hao.redis.integration.redis.RedisClient<String> redisClient(StringRedisTemplate stringRedisTemplate) {
        // 实现思路：
        // 1. 通过模板构建统一客户端封装。
        // 核心代码：实例化客户端封装
        return new RedisClientImpl(stringRedisTemplate);
    }

    /**
     * 启动时健康检查
     * <p>
     * 在 Spring 容器启动完成后，尝试连接 Redis 集群并打印所有可用节点。
     * 用于快速验证集群配置是否正确。
     *
     * 实现逻辑：
     * 1. 获取集群连接并打印节点信息。
     * 2. 捕获异常并记录失败原因。
     * 3. 确保连接使用后关闭，防止泄露。
     */
    @Bean
    public CommandLineRunner logClusterNodes(LettuceConnectionFactory factory) {
        return args -> {
            // 实现思路：
            // 1. 获取集群连接并输出节点信息。
            // 2. 异常时记录错误信息。
            // 3. 使用 try-with-resources 自动关闭连接。
            try (RedisClusterConnection connection = factory.getClusterConnection()) {
                log.info("Redis集群连接成功|Redis_cluster_connect_success");

                // 获取并遍历所有节点信息
                Iterable<RedisClusterNode> nodes = connection.clusterGetNodes();
                for (RedisClusterNode node : nodes) {
                    log.info("Redis集群节点信息|Redis_cluster_node_info,host={},port={},role={}",
                            node.getHost(), node.getPort(), node.getType());
                }
            } catch (Exception e) {
                log.error("Redis集群连接失败|Redis_cluster_connect_fail,error={}", e.getMessage(), e);
            }
        };
    }
}
