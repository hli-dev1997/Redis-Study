package com.hao.redis.service.impl;

import com.hao.redis.common.constants.DateConstants;
import com.hao.redis.common.enums.RedisKeysEnum;
import com.hao.redis.common.util.BloomFilterUtil;
import com.hao.redis.common.util.JsonUtil;
import com.hao.redis.dal.model.WeiboPost;
import com.hao.redis.integration.redis.RedisClient;
import com.hao.redis.service.WeiboService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

/**
 * 微博业务服务实现
 *
 * 类职责：
 * 实现用户注册、微博发布、互动与榜单查询等核心业务逻辑。
 *
 * 设计目的：
 * 1. 封装 Redis 读写流程，统一业务层行为。
 * 2. 聚合多种 Redis 数据结构以支撑微博业务场景。
 *
 * 为什么需要该类：
 * 业务逻辑需要集中实现，避免控制层直接操作 Redis。
 *
 * 核心实现思路：
 * - 使用全局发号器生成用户与微博ID。
 * - 使用 Hash 存储详情、List 构建时间轴、ZSet 构建热榜。
 * - JSON 序列化用于对象存储与传输。
 * <p>
 * 【性能调优 - 重点说明】
 * 1. 消除 N+1 查询：在该类的 listLatestPosts 和 getHotRank 中，通过 Pipeline (hmgetPipelined) 
 *    将原本需要多次往返的 HGET 操作聚合为一次，显著降低了首页加载的延迟。
 * 2. 存储优化：时间轴设计为仅存储 PostID 而非全量对象，降低了 List 结构的内存压力和同步开销。
 * 3. 拦截下沉：利用 BloomFilter 将非法请求拦截在业务逻辑最顶层，保护了昂贵的缓存读取和数据库回源操作。
 */
@Slf4j
@Service
public class WeiboServiceImpl implements WeiboService {

    @Autowired
    private RedisClient<String> redisClient;

    @Autowired
    private BloomFilterUtil bloomFilterUtil;

    /**
     * 注册新用户
     *
     * 实现逻辑：
     * 1. 使用全局发号器生成用户ID。
     * 2. 构造用户信息并写入 Redis 哈希。
     *
     * @param nickname 昵称
     * @param intro 简介
     * @return 新用户ID
     */
    @Override
    public Integer createUser(String nickname, String intro) {
        // 实现思路：
        // 1. 生成用户ID并写入用户信息。
        // 核心代码：生成用户ID
        Integer newUserId = redisClient.incr(RedisKeysEnum.GLOBAL_USER_ID.getKey()).intValue();
        // 构造用户信息并写入 Redis
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("userId", String.valueOf(newUserId)); // 冗余存一份 ID，便于查询
        paramMap.put("nickname", nickname);
        paramMap.put("intro", intro);
        paramMap.put("avatar", "default_head.png"); // 默认头像
        paramMap.put("fans", "0");    // 初始粉丝 0
        paramMap.put("follows", "0"); // 初始关注 0
        // 核心代码：写入用户信息
        redisClient.hmset(RedisKeysEnum.USER_PREFIX.join(newUserId), paramMap);
        
        // 核心代码：将 userId 加入布隆过滤器
        bloomFilterUtil.add("user", String.valueOf(newUserId));
        
        return newUserId;
    }

    /**
     * 获取用户详情
     *
     * 实现逻辑：
     * 1. 从 Redis 哈希中读取用户信息。
     *
     * @param userId 用户ID
     * @return 用户信息
     */
    @Override
    public Map<String, String> getUser(String userId) {
        // 核心代码：布隆过滤器前置校验
        if (!bloomFilterUtil.mightContain("user", userId)) {
            log.warn("布隆过滤器拦截非法用户请求|BloomFilter_block_user,userId={}", userId);
            return Collections.emptyMap();
        }

        // 实现思路：
        // 1. 通过用户ID读取用户哈希信息。
        // 核心代码：读取用户哈希
        return redisClient.hgetAll(RedisKeysEnum.USER_PREFIX.join(userId));
    }

    /**
     * 获取全站 UV
     *
     * 实现逻辑：
     * 1. 读取 UV 计数器并转换为整数。
     *
     * @return 全站 UV 数
     */
    @Override
    public Integer getTotalUV() {
        // 实现思路：
        // 1. 读取 UV 计数器并处理空值。
        String uv = redisClient.get(RedisKeysEnum.TOTAL_UV.getKey());
        // 如果是 null，就返回 0
        return uv == null ? 0 : Integer.parseInt(uv);
    }

    /**
     * 发布微博
     *
     * 实现逻辑：
     * 1. 生成微博ID并补全基础字段。
     * 2. 写入微博详情并追加到时间轴列表。
     *
     * @param userId 发布用户ID
     * @param body 微博内容
     * @return 微博ID
     */
    @Override
    public String createPost(String userId, WeiboPost body) {
        // 实现思路：
        // 1. 生成微博ID并补全字段。
        // 2. 写入详情与时间轴。
        // 核心代码：生成微博ID
        String postId = redisClient.incr(RedisKeysEnum.GLOBAL_POST_ID.getKey()).toString();
        // 补全对象属性
        body.setPostId(postId);
        body.setUserId(userId);
        // 优化：使用全局单例 DateTimeFormatter
        body.setCreateTime(LocalDateTime.now().format(DateConstants.STANDARD_DATETIME_FORMATTER));
        // 核心代码：序列化微博内容
        // 优化：使用 JsonUtil 工具类
        String objectValue = JsonUtil.toJson(body);
        // 核心代码：写入微博详情
        redisClient.hset(RedisKeysEnum.WEIBO_POST_INFO.getKey(), postId, objectValue);
        
        // 优化：时间轴只存 postId，减少内存占用和网络传输
        // 核心代码：写入时间轴 (仅存ID)
        redisClient.lpush(RedisKeysEnum.TIMELINE_KEY.getKey(), postId);
        // 优化：限制列表长度，防止无限增长 (保留最近 1000 条)
        redisClient.ltrim(RedisKeysEnum.TIMELINE_KEY.getKey(), 0, 999);

        // 核心代码：将 postId 加入布隆过滤器
        bloomFilterUtil.add("post", postId);
        
        return postId;
    }

    /**
     * 获取最新动态列表
     *
     * 实现逻辑：
     * 1. 读取时间轴列表（仅ID）。
     * 2. 批量获取微博详情。
     * 3. 反序列化为微博对象并过滤异常数据。
     *
     * @return 最新微博列表
     */
    @Override
    public List<WeiboPost> listLatestPosts() {
        // 实现思路：
        // 1. 读取时间轴列表 (ID列表)。
        // 2. 批量获取详情 (HMGET)。
        // 3. 反序列化。
        
        // 核心代码：读取时间轴 ID 列表
        List<String> postIds = redisClient.lrange(RedisKeysEnum.TIMELINE_KEY.getKey(), 0, 19);
        if (postIds == null || postIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        /**
         * 【性能优化点：Pipeline 批量获取】
         * 
         * 场景描述：
         * 此处需要根据 20 个微博 ID 去 Hash 表 WEIBO_POST_INFO 中查询详细内容。
         * 
         * 传统方案 (N+1)：
         * 遍历 postIds，循环调用 redisClient.hget()，会产生 20 次网络往返 (RTT)，
         * 假设单次频率 2ms，总耗时 = 20 * 2ms = 40ms。
         * 
         * 优化方案 (Pipeline)：
         * 使用 redisClient.hmgetPipelined()，将 20 条 hget 指令一次性打包发送给 Redis，
         * 只产生 1 次网络往返，总耗时 ≈ 2ms (加上少量服务端计算开销)。
         * 
         * 结论：相比原先循环调用，性能提升约 10-20 倍。
         */
        List<String> postJsonList = redisClient.hmgetPipelined(RedisKeysEnum.WEIBO_POST_INFO.getKey(), postIds);
        
        return postJsonList.stream()
                .filter(Objects::nonNull)
                .map(item -> {
                    // 使用 JsonUtil 反序列化
                    return JsonUtil.toBean(item, WeiboPost.class);
                })
                .filter(Objects::nonNull) // 过滤解析失败的数据
                .toList();
    }

    /**
     * 点赞微博
     *
     * 实现逻辑：
     * 1. 累加热搜排行榜分值。
     *
     * @param userId 点赞用户ID
     * @param postId 微博ID
     * @return 是否成功
     */
    @Override
    public Boolean likePost(String userId, String postId) {
        // 实现思路：
        // 1. 累加微博热度分数。
        // 核心代码：更新热度分数
        redisClient.zincrby(RedisKeysEnum.HOT_RANK_KEY.getKey(), 1, postId);
        return true;
    }

    /**
     * 获取全站热搜排行榜
     *
     * 实现逻辑：
     * 1. 读取热搜榜 Top 10 的微博ID列表。
     * 2. 批量加载微博详情。
     *
     * @return 热搜榜列表
     */
    @Override
    public List<WeiboPost> getHotRank() {
        // 实现思路：
        // 1. 获取热搜榜ID列表。
        // 2. 批量加载微博详情 (HMGET)。
        
        // 核心代码：读取热搜榜ID
        Set<String> topPostIds = redisClient.zrevrange(RedisKeysEnum.HOT_RANK_KEY.getKey(), 0, 9);
        if (topPostIds == null || topPostIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 【性能优化点】同上，使用 Pipeline 批量加载热搜列表详情
        List<String> postJsonList = redisClient.hmgetPipelined(
                RedisKeysEnum.WEIBO_POST_INFO.getKey(), 
                new ArrayList<>(topPostIds)
        );
        
        return postJsonList.stream()
                .filter(Objects::nonNull)
                .map(item -> JsonUtil.toBean(item, WeiboPost.class))
                .filter(Objects::nonNull)
                .toList();
    }

    /**
     * 获取微博详情
     *
     * 实现逻辑：
     * 1. 从 Redis 哈希读取微博详情字符串。
     * 2. 反序列化为微博对象并返回。
     *
     * @param postId 微博ID
     * @return 微博详情
     */
    public WeiboPost getWeiboPost(String postId) {
        // 1. 第一道防线：布隆过滤器前置校验
        if (!bloomFilterUtil.mightContain("post", postId)) {
            log.warn("布隆过滤器拦截非法请求|BloomFilter_block,postId={}", postId);
            return null;
        }

        // 2. 第二道防线：检查空值缓存（防止布隆误判导致的缓存穿透）
        // 如果空值缓存存在，说明之前已经查过数据库且不存在，直接返回 null
        String nullCacheKey = RedisKeysEnum.WEIBO_NULL_CACHE.join(postId);
        if (redisClient.exists(nullCacheKey)) {
            log.warn("命中空值缓存_拦截穿透请求|Null_cache_hit,postId={}", postId);
            return null;
        }

        // 3. 第三道防线：查询主缓存（Redis Hash）
        String postInfoStr = redisClient.hget(RedisKeysEnum.WEIBO_POST_INFO.getKey(), postId);
        if (postInfoStr != null) {
            return JsonUtil.toBean(postInfoStr, WeiboPost.class);
        }

        // 4. 第四道防线：回源查询数据库（模拟）
        // 真实场景：WeiboPost post = weiboMapper.selectById(postId);
        WeiboPost postFromDb = null; // 模拟数据库查不到

        if (postFromDb == null) {
            // 5. 核心逻辑：写入空值缓存
            // 既然布隆说存在，但数据库没有，说明发生了误判（或者数据刚被删除）
            // 写入一个短期的空值标记（如 5 分钟），防止短时间内重复打库
            log.warn("数据库查询为空_写入空值缓存|Db_miss_write_null_cache,postId={}", postId);
            redisClient.setex(nullCacheKey, 300, "1"); // 300秒过期
            return null;
        }

        // 如果数据库查到了，回写主缓存（略）
        return postFromDb;
    }
}
