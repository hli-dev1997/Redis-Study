package com.hao.redis.integration.redis;

import org.springframework.data.redis.core.Cursor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 统一 Redis 客户端接口
 *
 * 类职责：
 * 定义字符串、哈希、列表、集合、有序集合等常用命令的统一访问入口。
 *
 * 设计目的：
 * 1. 屏蔽底层客户端差异，提供一致的调用语义。
 * 2. 统一方法命名与参数校验口径，降低使用成本。
 *
 * 为什么需要该类：
 * Redis 操作类型众多，缺少统一接口会导致调用分散且难以维护。
 *
 * 核心实现思路：
 * - 按 Redis 数据类型分组定义方法。
 * - 实现层负责参数校验与模板调用。
 *
 * @param <T> 字符串操作的值类型（如 String 或序列化后的对象）
 */
@SuppressWarnings("all")
public interface RedisClient<T> {

    // 区域：字符串

    /**
     * 字符串 -> SET EX，写入并设置过期秒数。示例：SET user:1 "Tom" EX 60。
     */
    void set(String key, T value, int expireTime);

    /**
     * 字符串 -> SET，覆盖写入，无过期。示例：SET user:1 "Tom"。
     */
    void set(String key, T value);

    /**
     * 字符串 -> SETNX，不存在才写。示例：SETNX lock:task 1。
     *
     * @return 是否成功写入
     */
    Boolean setnx(String key, T value);

    /**
     * 字符串 -> SETEX，写入并设置过期。示例：SETEX captcha:123 300 "8391"。
     */
    void setex(String key, int expireSeconds, T value);

    /**
     * 【原子操作】分布式锁加锁
     * 对应原生命令: SET key value NX EX seconds
     *
     * @param key 锁键
     * @param value 锁值（通常是UUID，用于安全解锁）
     * @param expireTime 过期时间
     * @param unit 时间单位
     * @return 是否加锁成功
     */
    Boolean tryLock(String key, T value, long expireTime, TimeUnit unit);

    /**
     * 预防雪崩：写入并设置随机过期时间
     * <p>
     * 实现逻辑：
     * 在基础过期时间上增加一个随机偏移量（0~10%），防止大量 Key 同时过期。
     *
     * @param key 键
     * @param value 值
     * @param time 基础过期时间
     * @param unit 时间单位
     */
    void setWithRandomTtl(String key, T value, long time, TimeUnit unit);

    /**
     * 字符串 -> GET，读取值。示例：GET user:1。
     */
    T get(String key);

    /**
     * 字符串 -> SETBIT，设置位。示例：SETBIT mykey 7 1。
     */
    Boolean setBit(String key, long offset, boolean value);

    /**
     * 字符串 -> GETBIT，获取位。示例：GETBIT mykey 7。
     */
    Boolean getBit(String key, long offset);

    /**
     * 字符串 -> MGET，批量读取。示例：MGET user:1 user:2。
     */
    List<T> mget(String... keys);

    /**
     * 字符串 -> MSET，批量写入。示例：MSET a 1 b 2。
     */
    void mset(Map<String, T> values);

    /**
     * 字符串 -> GETSET，写新值返回旧值。示例：GETSET counter 0。
     */
    T getSet(String key, T value);

    /**
     * 字符串 -> EXISTS，判断键是否存在。示例：EXISTS user:1。
     */
    Boolean exists(String key);

    /**
     * 字符串 -> INCR，整数加 1。示例：INCR counter。
     */
    Long incr(String key);

    /**
     * 字符串 -> INCRBY，整数加指定值。示例：INCRBY counter 5。
     */
    Long incrBy(String key, long delta);

    /**
     * 字符串 -> INCRBYFLOAT，浮点加指定值。示例：INCRBYFLOAT price 1.5。
     */
    Double incrByFloat(String key, double delta);

    /**
     * 字符串 -> DECR，整数减 1。示例：DECR stock。
     */
    Long decr(String key);

    /**
     * 字符串 -> DECRBY，整数减指定值。示例：DECRBY stock 2。
     */
    Long decrBy(String key, long delta);

    /**
     * 字符串 -> APPEND，追加字符串。示例：APPEND log "line"。
     *
     * @return 追加后的新长度
     */
    Long append(String key, String appendValue);

    /**
     * 字符串 -> STRLEN，获取长度。示例：STRLEN user:bio。
     */
    Long strlen(String key);

    /**
     * 字符串 -> DEL，删除单个键。示例：DEL user:1。
     *
     * @return 删除的键数量（0/1）
     */
    Long del(String key);

    /**
     * 字符串 -> DEL，删除多个键。示例：DEL a b c。
     *
     * @return 删除的键数量
     */
    Long del(String... keys);

    // 区域结束

    // 区域：哈希

    /**
     * 哈希 -> HSET，设置字段。示例：HSET user:1 name "Tom"。
     */
    void hset(String key, String field, T value);

    /**
     * 哈希 -> HSETNX，字段不存在才写。示例：HSETNX user:1 name "Tom"。
     */
    Boolean hsetnx(String key, String field, T value);

    /**
     * 哈希 -> HGET，读取字段。示例：HGET user:1 name。
     */
    T hget(String key, String field);

    /**
     * 哈希 -> HGETALL，获取全部字段。示例：HGETALL user:1。
     */
    Map<String, T> hgetAll(String key);

    /**
     * 哈希 -> HMSET，批量写字段。示例：HMSET user:1 name Tom age 18。
     */
    void hmset(String key, Map<String, T> paramMap);

    /**
     * 哈希 -> HMGET，批量读字段。示例：HMGET user:1 name age。
     */
    List<T> hmget(String key, String... fields);
    
    /**
     * 哈希 -> HMGET，批量读字段（List 参数重载）。示例：HMGET user:1 [name, age]。
     */
    List<T> hmget(String key, List<String> fields);

    /**
     * 哈希 -> HKEYS，列出字段名。示例：HKEYS user:1。
     */
    Set<String> hkeys(String key);

    /**
     * 哈希 -> HVALS，列出字段值。示例：HVALS user:1。
     */
    List<T> hvals(String key);

    /**
     * 哈希 -> HLEN，字段数量。示例：HLEN user:1。
     */
    Long hlen(String key);

    /**
     * 哈希 -> HEXISTS，判断字段存在。示例：HEXISTS user:1 name。
     */
    Boolean hexists(String key, String field);

    /**
     * 哈希 -> HDEL，删除字段。示例：HDEL user:1 name age。
     *
     * @return 删除的字段数
     */
    Long hdel(String key, String... fields);

    /**
     * 哈希 -> HINCRBY，整数字段自增。示例：HINCRBY user:1 score 10。
     */
    Long hincrBy(String key, String field, long delta);

    /**
     * 哈希 -> HINCRBYFLOAT，浮点字段自增。示例：HINCRBYFLOAT user:1 price 1.5。
     */
    Double hincrByFloat(String key, String field, double delta);

    // 区域结束

    // 区域：列表

    /**
     * 列表 -> LPUSH，左侧入队。示例：LPUSH queue a b。
     */
    Long lpush(String key, T... values);

    /**
     * 列表 -> RPUSH，右侧入队。示例：RPUSH queue a b。
     */
    Long rpush(String key, T... values);

    /**
     * 列表 -> LPOP，左出队。示例：LPOP queue。
     */
    T lpop(String key);

    /**
     * 列表 -> RPOP，右出队。示例：RPOP queue。
     */
    T rpop(String key);

    /**
     * 列表 -> BLPOP，阻塞左出队。示例：BLPOP 5 queue。
     *
     * @return 键值对，超时返回null
     */
    List<String> blpop(int timeoutSeconds, String... keys);

    /**
     * 列表 -> BRPOP，阻塞右出队。示例：BRPOP 5 queue。
     *
     * @return 键值对，超时返回null
     */
    List<String> brpop(int timeoutSeconds, String... keys);

    /**
     * 列表 -> LRANGE，区间读取。示例：LRANGE queue 0 9。
     */
    List<T> lrange(String key, long start, long stop);

    /**
     * 列表 -> LINDEX，按索引取值。示例：LINDEX queue 0。
     */
    T lindex(String key, long index);

    /**
     * 列表 -> LSET，按索引覆盖。示例：LSET queue 0 "new"。
     */
    void lset(String key, long index, T value);

    /**
     * 列表 -> LTRIM，保留指定区间。示例：LTRIM queue 0 9。
     */
    void ltrim(String key, long start, long stop);

    /**
     * 列表 -> LREM，按值删除。示例：LREM queue 1 "a"。
     */
    Long lrem(String key, long count, T value);

    /**
     * 列表 -> RPOPLPUSH，尾取头插。示例：RPOPLPUSH src dest。
     */
    T rpoplpush(String sourceKey, String destinationKey);

    /**
     * 列表 -> LLEN，长度查询。示例：LLEN queue。
     */
    Long llen(String key);

    // 区域结束

    // 区域：无序集合

    /**
     * 无序集合 -> SADD，添加成员。示例：SADD tags a b。
     *
     * @return 新增的成员数
     */
    Long sadd(String key, T... members);

    /**
     * 无序集合 -> SREM，移除成员。示例：SREM tags a b。
     *
     * @return 移除的成员数
     */
    Long srem(String key, T... members);

    /**
     * 无序集合 -> SMEMBERS，获取全部成员。示例：SMEMBERS tags。
     */
    Set<T> smembers(String key);

    /**
     * 无序集合 -> SISMEMBER，判断成员存在。示例：SISMEMBER tags a。
     */
    Boolean sismember(String key, T member);

    /**
     * 无序集合 -> SCARD，成员数量。示例：SCARD tags。
     */
    Long scard(String key);

    /**
     * 无序集合 -> SPOP，随机弹出一个。示例：SPOP tags。
     */
    T spop(String key);

    /**
     * 无序集合 -> SPOP count，随机弹出多个。示例：SPOP tags 2。
     */
    Set<T> spop(String key, long count);

    /**
     * 无序集合 -> SRANDMEMBER，随机返回一个但不删除。示例：SRANDMEMBER tags。
     */
    T srandmember(String key);

    /**
     * 无序集合 -> SRANDMEMBER count，随机返回多个可重复。示例：SRANDMEMBER tags 3。
     */
    List<T> srandmember(String key, int count);

    /**
     * 无序集合 -> SINTER，求交集。示例：SINTER a b。
     */
    Set<T> sinter(String... keys);

    /**
     * 无序集合 -> SUNION，求并集。示例：SUNION a b。
     */
    Set<T> sunion(String... keys);

    /**
     * 无序集合 -> SDIFF，差集。示例：SDIFF a b。
     */
    Set<T> sdiff(String... keys);

    /**
     * 无序集合 -> SINTERSTORE，交集存储到目标。示例：SINTERSTORE dest a b。
     *
     * @return 结果集大小
     */
    Long sinterstore(String destination, String... keys);

    /**
     * 无序集合 -> SUNIONSTORE，并集存储到目标。示例：SUNIONSTORE dest a b。
     *
     * @return 结果集大小
     */
    Long sunionstore(String destination, String... keys);

    /**
     * 无序集合 -> SDIFFSTORE，差集存储到目标。示例：SDIFFSTORE dest a b。
     *
     * @return 结果集大小
     */
    Long sdiffstore(String destination, String... keys);

    // 区域结束：无序集合

    // 区域：有序集合

    /**
     * 有序集合 -> ZADD，批量新增成员。示例：ZADD rank 100 user1 200 user2。
     *
     * @return 新增的成员数
     */
    Long zadd(String key, Map<T, Double> valueMap);

    /**
     * 有序集合 -> ZADD，新增单个成员。示例：ZADD rank 100 user1。
     *
     * @return 1 表示新增，0 表示更新
     */
    Long zadd(String key, double score, T member);

    /**
     * 有序集合 -> ZRANGE，按索引升序取。示例：ZRANGE rank 0 9。
     */
    Set<T> zrange(String key, long start, long stop);

    /**
     * 有序集合 -> ZREVRANGE，按索引降序取。示例：ZREVRANGE rank 0 9。
     */
    Set<T> zrevrange(String key, long start, long stop);

    /**
     * 有序集合 -> ZRANGEBYSCORE，按分数升序区间。示例：ZRANGEBYSCORE rank 0 100。
     */
    Set<T> zrangeByScore(String key, double minScore, double maxScore);

    /**
     * 有序集合 -> ZREVRANGEBYSCORE，按分数降序区间。示例：ZREVRANGEBYSCORE rank 100 0。
     */
    Set<T> zrevrangeByScore(String key, double maxScore, double minScore);

    /**
     * 有序集合 -> ZRANK，升序排名。示例：ZRANK rank user1。
     */
    Long zrank(String key, T member);

    /**
     * 有序集合 -> ZREVRANK，降序排名。示例：ZREVRANK rank user1。
     */
    Long zrevrank(String key, T member);

    /**
     * 有序集合 -> ZREMRANGEBYSCORE，按分数区间删除。示例：ZREMRANGEBYSCORE rank 0 50。
     *
     * @return 删除的成员数
     */
    Long zremrangeByScore(String key, double scoreMin, double scoreMax);

    /**
     * 有序集合 -> ZREMRANGEBYRANK，按索引区间删除。示例：ZREMRANGEBYRANK rank 0 1。
     *
     * @return 删除的成员数
     */
    Long zremrangeByRank(String key, long start, long stop);

    /**
     * 有序集合 -> ZREM，删除指定成员。示例：ZREM rank user1 user2。
     *
     * @return 删除的成员数
     */
    Long zrem(String key, T... members);

    /**
     * 有序集合 -> ZSCORE，查询成员分数。示例：ZSCORE rank user1。
     */
    Double zscore(String key, T member);

    /**
     * 有序集合 -> ZINCRBY，分数自增。示例：ZINCRBY rank 10 user1。
     */
    Double zincrby(String key, double increment, T member);

    /**
     * 有序集合 -> ZCARD，成员数量。示例：ZCARD rank。
     */
    Long zcard(String key);

    /**
     * 有序集合 -> ZCOUNT，按分数区间计数。示例：ZCOUNT rank 0 100。
     */
    Long zcount(String key, double scoreMin, double scoreMax);

    /**
     * 有序集合 -> ZPOPMIN，按分数从小到大弹出成员。示例：ZPOPMIN rank 2。
     *
     * @return 弹出的成员集合（含 count 个）
     */
    Set<T> zpopmin(String key, long count);

    /**
     * 有序集合 -> ZPOPMAX，按分数从大到小弹出成员。示例：ZPOPMAX rank 2。
     *
     * @return 弹出的成员集合（含 count 个）
     */
    Set<T> zpopmax(String key, long count);

    /**
     * 有序集合 -> ZINTERSTORE，将多个有序集合的交集存入目标键。示例：ZINTERSTORE dest 2 a b。
     *
     * @return 结果集成员数量
     */
    Long zinterstore(String destination, String... keys);

    /**
     * 有序集合 -> ZUNIONSTORE，将多个有序集合的并集存入目标键。示例：ZUNIONSTORE dest 2 a b。
     *
     * @return 结果集成员数量
     */
    Long zunionstore(String destination, String... keys);

    // 区域结束：有序集合

    // 区域：过期控制

    /**
     * 键过期 -> EXPIRE，设置秒级过期。示例：EXPIRE user:1 60。
     */
    Boolean expire(String key, int seconds);

    /**
     * 键过期 -> EXPIREAT，按时间戳过期。示例：EXPIREAT user:1 1700000000。
     */
    Boolean expireAt(String key, long timestamp);

    /**
     * 键过期 -> PERSIST，移除过期时间。示例：PERSIST user:1。
     */
    Boolean persist(String key);

    /**
     * 键过期 -> TTL，查看剩余秒数。示例：TTL user:1。
     */
    Long ttl(String key);

    // 区域结束

    // 区域：通用

    /**
     * 通用 -> TYPE，查看数据类型。示例：TYPE user:1。
     */
    String type(String key);

    /**
     * 通用 -> RENAME，强制重命名。示例：RENAME a b。
     */
    void rename(String oldKey, String newKey);

    /**
     * 通用 -> RENAMENX，目标不存在时重命名。示例：RENAMENX a b。
     */
    Boolean renamenx(String oldKey, String newKey);

    /**
     * 通用 -> KEYS，模式匹配列出键（生产慎用）。示例：KEYS user:*。
     */
    Set<String> keys(String pattern);

    /**
     * 通用 -> SCAN，迭代遍历键。示例：SCAN 0 MATCH user:* COUNT 100。
     */
    Cursor<String> scan(String pattern, long count);

    /**
     * 【性能优化】执行 Pipeline 批量操作
     * <p>
     * 使用场景：
     * 当需要连续执行多个非依赖的 Redis 命令时，通过 Pipeline 可以将命令一次性发送给服务端，
     * 极大减少网络 RTT（往返时延）带来的开销。
     *
     * @param callback Pipeline 回调逻辑
     * @param <R>      返回类型封装
     * @return 执行结果列表
     */
    <R> List<R> executePipelined(org.springframework.data.redis.core.RedisCallback<?> callback);

    /**
     * 【性能优化】批量从 Hash 中获取指定字段（Pipeline 版本）
     * <p>
     * 优势：
     * 与循环调用 hget 相比，该方法只产生一次网络请求，性能提升 5-10 倍。
     *
     * @param key       Hash 键
     * @param fieldList 字段列表
     * @return 按 fieldList 顺序排列的值列表
     */
    List<T> hmgetPipelined(String key, List<String> fieldList);

    // 区域结束
}
