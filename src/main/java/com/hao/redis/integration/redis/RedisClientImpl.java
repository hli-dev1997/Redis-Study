package com.hao.redis.integration.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * RedisClient 接口实现
 *
 * 类职责：
 * 基于 StringRedisTemplate 封装 Redis 常用命令，并提供参数校验与通用工具能力。
 *
 * 设计目的：
 * 1. 统一 Redis 调用方式，减少业务层重复代码。
 * 2. 规范参数校验与返回结果处理，提高稳定性。
 *
 * 为什么需要该类：
 * 直接使用模板容易分散调用逻辑，集中封装便于维护与扩展。
 *
 * 核心实现思路：
 * - 各方法按数据结构映射 Redis 原生命令。
 * - 统一进行参数校验与空值处理。
 */
@Slf4j
public class RedisClientImpl implements RedisClient<String> {

    private final StringRedisTemplate redisTemplate;

    public RedisClientImpl(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /* ------------------ 辅助校验 ------------------ */
    /**
     * 校验字符串参数
     *
     * 实现逻辑：
     * 1. 判空与空白校验。
     * 2. 不满足条件时抛出异常。
     *
     * @param key 待校验参数
     * @param name 参数名称
     */
    private void validateKey(String key, String name) {
        // 实现思路：
        // 1. 使用工具类判断字符串有效性。
        if (!StringUtils.hasText(key)) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
    }

    /**
     * 校验数组参数
     *
     * 实现逻辑：
     * 1. 判空并校验长度。
     * 2. 不满足条件时抛出异常。
     *
     * @param params 参数数组
     * @param name 参数名称
     */
    private void validateParams(Object[] params, String name) {
        // 实现思路：
        // 1. 校验参数数组非空且长度大于 0。
        if (params == null || params.length == 0) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
    }
    
    /**
     * 校验集合参数
     *
     * 实现逻辑：
     * 1. 判空并校验大小。
     * 2. 不满足条件时抛出异常。
     *
     * @param collection 参数集合
     * @param name 参数名称
     */
    private void validateCollection(Collection<?> collection, String name) {
        // 实现思路：
        // 1. 校验集合非空且大小大于 0。
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
    }

    /**
     * 校验正数参数
     *
     * 实现逻辑：
     * 1. 校验数值大于 0。
     * 2. 不满足条件时抛出异常。
     *
     * @param value 参数值
     * @param name 参数名称
     */
    private void validatePositive(long value, String name) {
        // 实现思路：
        // 1. 校验参数是否为正数。
        if (value <= 0) {
            throw new IllegalArgumentException(name + " 必须大于 0");
        }
    }

    // 区域：字符串

    /** 字符串 -> SET EX：写入并设置过期秒数。示例：SET user:1 "Tom" EX 60。 */
    @Override
    public void set(String key, String value, int expireTime) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        validatePositive(expireTime, "expireTime");
        redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(expireTime));
    }

    /** 字符串 -> SET：覆盖写入，无过期。示例：SET user:1 "Tom"。 */
    @Override
    public void set(String key, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        redisTemplate.opsForValue().set(key, value);
    }

    /** 字符串 -> SETNX：不存在才写。示例：SETNX lock:task 1。 */
    @Override
    public Boolean setnx(String key, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        return redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    /** 字符串 -> SETEX：写入并设置过期。示例：SETEX captcha:123 300 "8391"。 */
    @Override
    public void setex(String key, int expireSeconds, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        validatePositive(expireSeconds, "expireSeconds");
        redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(expireSeconds));
    }

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
    @Override
    public Boolean tryLock(String key, String value, long expireTime, TimeUnit unit) {
        validateKey(key, "key");
        validateKey(value, "value");
        validatePositive(expireTime, "expireTime");
        return redisTemplate.opsForValue().setIfAbsent(key, value, expireTime, unit);
    }

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
    @Override
    public void setWithRandomTtl(String key, String value, long time, TimeUnit unit) {
        validateKey(key, "key");
        validateKey(value, "value");
        validatePositive(time, "time");

        // 1. 计算随机偏移量 (0 ~ 10% 的基础时间)
        // 使用 ThreadLocalRandom 避免多线程竞争
        // 修正：确保即使 time 很小，也能产生随机效果
        long randomBound = Math.max(1, time / 10);
        long offset = ThreadLocalRandom.current().nextLong(randomBound);
        
        // 2. 计算最终过期时间
        long finalTime = time + offset;
        
        // 3. 写入 Redis
        redisTemplate.opsForValue().set(key, value, finalTime, unit);
        
        log.debug("预防雪崩设置完成|Avalanche_protection, key={}, baseTtl={}, finalTtl={}", key, time, finalTime);
    }

    /** 字符串 -> GET：读取值。示例：GET user:1。 */
    @Override
    public String get(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().get(key);
    }

    /** 字符串 -> SETBIT：设置位。示例：SETBIT mykey 7 1。 */
    @Override
    public Boolean setBit(String key, long offset, boolean value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        if (offset < 0) {
            throw new IllegalArgumentException("offset 不能为负数");
        }
        return redisTemplate.opsForValue().setBit(key, offset, value);
    }

    /** 字符串 -> GETBIT：获取位。示例：GETBIT mykey 7。 */
    @Override
    public Boolean getBit(String key, long offset) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        if (offset < 0) {
            throw new IllegalArgumentException("offset 不能为负数");
        }
        return redisTemplate.opsForValue().getBit(key, offset);
    }

    /** 字符串 -> MGET：批量读取。示例：MGET user:1 user:2。 */
    @Override
    public List<String> mget(String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateParams(keys, "keys");
        List<String> result = redisTemplate.opsForValue().multiGet(Arrays.asList(keys));
        return result != null ? result : Collections.emptyList();
    }

    /** 字符串 -> MSET：批量写入。示例：MSET a 1 b 2。 */
    @Override
    public void mset(Map<String, String> values) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values 不能为空");
        }
        redisTemplate.opsForValue().multiSet(values);
    }

    /** 字符串 -> GETSET：写新值返回旧值。示例：GETSET counter 0。 */
    @Override
    public String getSet(String key, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        return redisTemplate.opsForValue().getAndSet(key, value);
    }

    /** 字符串 -> EXISTS：判断 key 是否存在。示例：EXISTS user:1。 */
    @Override
    public Boolean exists(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.hasKey(key);
    }

    /** 字符串 -> INCR：整数加 1。示例：INCR counter。 */
    @Override
    public Long incr(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().increment(key);
    }

    /** 字符串 -> INCRBY：整数加指定值。示例：INCRBY counter 5。 */
    @Override
    public Long incrBy(String key, long delta) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /** 字符串 -> INCRBYFLOAT：浮点加指定值。示例：INCRBYFLOAT price 1.5。 */
    @Override
    public Double incrByFloat(String key, double delta) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /** 字符串 -> DECR：整数减 1。示例：DECR stock。 */
    @Override
    public Long decr(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().decrement(key);
    }

    /** 字符串 -> DECRBY：整数减指定值。示例：DECRBY stock 2。 */
    @Override
    public Long decrBy(String key, long delta) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().decrement(key, delta);
    }

    /** 字符串 -> APPEND：追加字符串。示例：APPEND log "line"。 */
    @Override
    public Long append(String key, String appendValue) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(appendValue, "appendValue");
        Integer appended = redisTemplate.opsForValue().append(key, appendValue);
        return appended == null ? 0L : appended.longValue();
    }

    /** 字符串 -> STRLEN：获取长度。示例：STRLEN user:bio。 */
    @Override
    public Long strlen(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForValue().size(key);
    }

    /** 字符串 -> DEL：删除单个 key。示例：DEL user:1。 */
    @Override
    public Long del(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Boolean deleted = redisTemplate.delete(key);
        return Boolean.TRUE.equals(deleted) ? 1L : 0L;
    }

    /** 字符串 -> DEL：删除多个 key。示例：DEL a b c。 */
    @Override
    public Long del(String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateParams(keys, "keys");
        return redisTemplate.delete(Arrays.asList(keys));
    }

    // 区域结束

    // 区域：哈希

    /** 哈希 -> HSET：设置字段。示例：HSET user:1 name "Tom"。 */
    @Override
    public void hset(String key, String field, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        validateKey(value, "value");
        redisTemplate.opsForHash().put(key, field, value);
    }

    /** 哈希 -> HSETNX：字段不存在才写。示例：HSETNX user:1 name "Tom"。 */
    @Override
    public Boolean hsetnx(String key, String field, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        validateKey(value, "value");
        return redisTemplate.opsForHash().putIfAbsent(key, field, value);
    }

    /** 哈希 -> HGET：读取字段。示例：HGET user:1 name。 */
    @Override
    public String hget(String key, String field) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        Object val = redisTemplate.opsForHash().get(key, field);
        return val != null ? val.toString() : null;
    }

    /** 哈希 -> HGETALL：获取全部字段。示例：HGETALL user:1。 */
    @Override
    public Map<String, String> hgetAll(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
        if (entries.isEmpty()) {
            return Collections.emptyMap();
        }
        return entries.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString(),
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /** 哈希 -> HMSET：批量写字段。示例：HMSET user:1 name Tom age 18。 */
    @Override
    public void hmset(String key, Map<String, String> paramMap) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        if (paramMap == null || paramMap.isEmpty()) {
            throw new IllegalArgumentException("paramMap 不能为空");
        }
        redisTemplate.opsForHash().putAll(key, paramMap);
    }

    /** 哈希 -> HMGET：批量读字段。示例：HMGET user:1 name age。 */
    @Override
    public List<String> hmget(String key, String... fields) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(fields, "fields");
        List<Object> vals = redisTemplate.opsForHash().multiGet(key, Arrays.asList(fields));
        return vals.stream().map(v -> v != null ? v.toString() : null).collect(Collectors.toList());
    }
    
    /** 哈希 -> HMGET：批量读字段（List 参数重载）。示例：HMGET user:1 [name, age]。 */
    @Override
    public List<String> hmget(String key, List<String> fields) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateCollection(fields, "fields");
        List<Object> vals = redisTemplate.opsForHash().multiGet(key, new ArrayList<>(fields));
        return vals.stream().map(v -> v != null ? v.toString() : null).collect(Collectors.toList());
    }

    /** 哈希 -> HKEYS：列出字段名。示例：HKEYS user:1。 */
    @Override
    public Set<String> hkeys(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<Object> keys = redisTemplate.opsForHash().keys(key);
        if (keys == null || keys.isEmpty()) {
            return Collections.emptySet();
        }
        return keys.stream().map(Object::toString).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** 哈希 -> HVALS：列出字段值。示例：HVALS user:1。 */
    @Override
    public List<String> hvals(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        List<Object> vals = redisTemplate.opsForHash().values(key);
        if (vals == null || vals.isEmpty()) {
            return Collections.emptyList();
        }
        return vals.stream().map(v -> v != null ? v.toString() : null).collect(Collectors.toList());
    }

    /** 哈希 -> HLEN：字段数量。示例：HLEN user:1。 */
    @Override
    public Long hlen(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForHash().size(key);
    }

    /** 哈希 -> HEXISTS：判断字段存在。示例：HEXISTS user:1 name。 */
    @Override
    public Boolean hexists(String key, String field) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        return redisTemplate.opsForHash().hasKey(key, field);
    }

    /** 哈希 -> HDEL：删除字段。示例：HDEL user:1 name age。 */
    @Override
    public Long hdel(String key, String... fields) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(fields, "fields");
        return redisTemplate.opsForHash().delete(key, (Object[]) fields);
    }

    /** 哈希 -> HINCRBY：整数字段自增。示例：HINCRBY user:1 score 10。 */
    @Override
    public Long hincrBy(String key, String field, long delta) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        return redisTemplate.opsForHash().increment(key, field, delta);
    }

    /** 哈希 -> HINCRBYFLOAT：浮点字段自增。示例：HINCRBYFLOAT user:1 price 1.5。 */
    @Override
    public Double hincrByFloat(String key, String field, double delta) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(field, "field");
        return redisTemplate.opsForHash().increment(key, field, delta);
    }

    // 区域结束

    // 区域：列表

    /** 列表 -> LPUSH：左侧入队。示例：LPUSH queue a b。 */
    @Override
    public Long lpush(String key, String... values) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(values, "values");
        return redisTemplate.opsForList().leftPushAll(key, values);
    }

    /** 列表 -> RPUSH：右侧入队。示例：RPUSH queue a b。 */
    @Override
    public Long rpush(String key, String... values) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(values, "values");
        return redisTemplate.opsForList().rightPushAll(key, values);
    }

    /** 列表 -> LPOP：左出队。示例：LPOP queue。 */
    @Override
    public String lpop(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForList().leftPop(key);
    }

    /** 列表 -> RPOP：右出队。示例：RPOP queue。 */
    @Override
    public String rpop(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForList().rightPop(key);
    }

    /** 列表 -> BLPOP：阻塞左出队。示例：BLPOP 5 queue。 */
    @Override
    public List<String> blpop(int timeoutSeconds, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 使用 RedisTemplate 的 execute 方法调用底层连接的 bLPop。
        // 3. 这样可以实现真正的原子性多 Key 监听。
        validateParams(keys, "keys");
        validatePositive(timeoutSeconds, "timeoutSeconds");

        // 核心修复：使用 execute 调用底层 bLPop
        return redisTemplate.execute((RedisCallback<List<String>>) connection -> {
            byte[][] keyBytes = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                keyBytes[i] = keys[i].getBytes(StandardCharsets.UTF_8);
            }
            List<byte[]> result = connection.bLPop(timeoutSeconds, keyBytes);
            if (result == null || result.isEmpty()) {
                return null;
            }
            // bLPop 返回的第一个元素是 key，第二个是 value
            return Arrays.asList(
                    new String(result.get(0), StandardCharsets.UTF_8),
                    new String(result.get(1), StandardCharsets.UTF_8)
            );
        });
    }

    /** 列表 -> BRPOP：阻塞右出队。示例：BRPOP 5 queue。 */
    @Override
    public List<String> brpop(int timeoutSeconds, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 使用 RedisTemplate 的 execute 方法调用底层连接的 bRPop。
        validateParams(keys, "keys");
        validatePositive(timeoutSeconds, "timeoutSeconds");

        // 核心修复：使用 execute 调用底层 bRPop
        return redisTemplate.execute((RedisCallback<List<String>>) connection -> {
            byte[][] keyBytes = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                keyBytes[i] = keys[i].getBytes(StandardCharsets.UTF_8);
            }
            List<byte[]> result = connection.bRPop(timeoutSeconds, keyBytes);
            if (result == null || result.isEmpty()) {
                return null;
            }
            // bRPop 返回的第一个元素是 key，第二个是 value
            return Arrays.asList(
                    new String(result.get(0), StandardCharsets.UTF_8),
                    new String(result.get(1), StandardCharsets.UTF_8)
            );
        });
    }

    /** 列表 -> LRANGE：区间读取。示例：LRANGE queue 0 9。 */
    @Override
    public List<String> lrange(String key, long start, long stop) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        List<String> result = redisTemplate.opsForList().range(key, start, stop);
        return result != null ? result : Collections.emptyList();
    }

    /** 列表 -> LINDEX：按索引取值。示例：LINDEX queue 0。 */
    @Override
    public String lindex(String key, long index) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForList().index(key, index);
    }

    /** 列表 -> LSET：按索引覆盖。示例：LSET queue 0 "new"。 */
    @Override
    public void lset(String key, long index, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        redisTemplate.opsForList().set(key, index, value);
    }

    /** 列表 -> LTRIM：保留指定区间。示例：LTRIM queue 0 9。 */
    @Override
    public void ltrim(String key, long start, long stop) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        redisTemplate.opsForList().trim(key, start, stop);
    }

    /** 列表 -> LREM：按值删除。示例：LREM queue 1 "a"。 */
    @Override
    public Long lrem(String key, long count, String value) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(value, "value");
        return redisTemplate.opsForList().remove(key, count, value);
    }

    /** 列表 -> RPOPLPUSH：尾取头插。示例：RPOPLPUSH src dest。 */
    @Override
    public String rpoplpush(String sourceKey, String destinationKey) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(sourceKey, "sourceKey");
        validateKey(destinationKey, "destinationKey");
        return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey);
    }

    /** 列表 -> LLEN：长度查询。示例：LLEN queue。 */
    @Override
    public Long llen(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForList().size(key);
    }

    // 区域结束

    // 区域：无序集合

    /** 无序集合 -> SADD：添加成员。示例：SADD tags a b。 */
    @Override
    public Long sadd(String key, String... members) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(members, "members");
        return redisTemplate.opsForSet().add(key, members);
    }

    /** 无序集合 -> SREM：移除成员。示例：SREM tags a b。 */
    @Override
    public Long srem(String key, String... members) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(members, "members");
        return redisTemplate.opsForSet().remove(key, (Object[]) members);
    }

    /** 无序集合 -> SMEMBERS：获取全部成员。示例：SMEMBERS tags。 */
    @Override
    public Set<String> smembers(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<String> result = redisTemplate.opsForSet().members(key);
        return result != null ? result : Collections.emptySet();
    }

    /** 无序集合 -> SISMEMBER：判断成员存在。示例：SISMEMBER tags a。 */
    @Override
    public Boolean sismember(String key, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        return redisTemplate.opsForSet().isMember(key, member);
    }

    /** 无序集合 -> SCARD：成员数量。示例：SCARD tags。 */
    @Override
    public Long scard(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForSet().size(key);
    }

    /** 无序集合 -> SPOP：随机弹出一个。示例：SPOP tags。 */
    @Override
    public String spop(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForSet().pop(key);
    }

    /** 无序集合 -> SPOP count：随机弹出多个。示例：SPOP tags 2。 */
    @Override
    public Set<String> spop(String key, long count) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validatePositive(count, "count");
        Collection<String> result = redisTemplate.opsForSet().pop(key, count);
        if (result == null || result.isEmpty()) {
            return Collections.emptySet();
        }
        return new LinkedHashSet<>(result);
    }

    /** 无序集合 -> SRANDMEMBER：随机返回一个但不删除。示例：SRANDMEMBER tags。 */
    @Override
    public String srandmember(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForSet().randomMember(key);
    }

    /** 无序集合 -> SRANDMEMBER count：随机返回多个可重复。示例：SRANDMEMBER tags 3。 */
    @Override
    public List<String> srandmember(String key, int count) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForSet().randomMembers(key, count);
    }

    /** 无序集合 -> SINTER：求交集。示例：SINTER a b。 */
    @Override
    public Set<String> sinter(String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateParams(keys, "keys");
        if (keys.length == 1) {
            Set<String> result = redisTemplate.opsForSet().members(keys[0]);
            return result != null ? result : Collections.emptySet();
        }
        return redisTemplate.opsForSet().intersect(keys[0], Arrays.asList(keys).subList(1, keys.length));
    }

    /** 无序集合 -> SUNION：求并集。示例：SUNION a b。 */
    @Override
    public Set<String> sunion(String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateParams(keys, "keys");
        if (keys.length == 1) {
            Set<String> result = redisTemplate.opsForSet().members(keys[0]);
            return result != null ? result : Collections.emptySet();
        }
        return redisTemplate.opsForSet().union(keys[0], Arrays.asList(keys).subList(1, keys.length));
    }

    /** 无序集合 -> SDIFF：差集。示例：SDIFF a b。 */
    @Override
    public Set<String> sdiff(String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateParams(keys, "keys");
        if (keys.length == 1) {
            Set<String> result = redisTemplate.opsForSet().members(keys[0]);
            return result != null ? result : Collections.emptySet();
        }
        return redisTemplate.opsForSet().difference(keys[0], Arrays.asList(keys).subList(1, keys.length));
    }

    /** 无序集合 -> SINTERSTORE：交集存储到目标。示例：SINTERSTORE dest a b。 */
    @Override
    public Long sinterstore(String destination, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(destination, "destination");
        validateParams(keys, "keys");
        String first = keys[0];
        List<String> others = keys.length > 1 ? Arrays.asList(keys).subList(1, keys.length) : Collections.emptyList();
        return redisTemplate.opsForSet().intersectAndStore(first, others, destination);
    }

    /** 无序集合 -> SUNIONSTORE：并集存储到目标。示例：SUNIONSTORE dest a b。 */
    @Override
    public Long sunionstore(String destination, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(destination, "destination");
        validateParams(keys, "keys");
        String first = keys[0];
        List<String> others = keys.length > 1 ? Arrays.asList(keys).subList(1, keys.length) : Collections.emptyList();
        return redisTemplate.opsForSet().unionAndStore(first, others, destination);
    }

    /** 无序集合 -> SDIFFSTORE：差集存储到目标。示例：SDIFFSTORE dest a b。 */
    @Override
    public Long sdiffstore(String destination, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(destination, "destination");
        validateParams(keys, "keys");
        String first = keys[0];
        List<String> others = keys.length > 1 ? Arrays.asList(keys).subList(1, keys.length) : Collections.emptyList();
        return redisTemplate.opsForSet().differenceAndStore(first, others, destination);
    }

    // 区域结束：无序集合

    // 区域：有序集合

    /** 有序集合 -> ZADD：批量新增成员。示例：ZADD rank 100 user1 200 user2。 */
    @Override
    public Long zadd(String key, Map<String, Double> valueMap) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        if (valueMap == null || valueMap.isEmpty()) {
            throw new IllegalArgumentException("valueMap 不能为空");
        }
        Set<ZSetOperations.TypedTuple<String>> tuples = valueMap.entrySet().stream()
                .map(e -> ZSetOperations.TypedTuple.of(e.getKey(), e.getValue()))
                .collect(Collectors.toSet());
        return redisTemplate.opsForZSet().add(key, tuples);
    }

    /** 有序集合 -> ZADD：新增单个成员。示例：ZADD rank 100 user1。 */
    @Override
    public Long zadd(String key, double score, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        Boolean added = redisTemplate.opsForZSet().add(key, member, score);
        return Boolean.TRUE.equals(added) ? 1L : 0L;
    }

    /** 有序集合 -> ZRANGE：按索引升序取。示例：ZRANGE rank 0 9。 */
    @Override
    public Set<String> zrange(String key, long start, long stop) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<String> result = redisTemplate.opsForZSet().range(key, start, stop);
        return result != null ? result : Collections.emptySet();
    }

    /** 有序集合 -> ZREVRANGE：按索引降序取。示例：ZREVRANGE rank 0 9。 */
    @Override
    public Set<String> zrevrange(String key, long start, long stop) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<String> result = redisTemplate.opsForZSet().reverseRange(key, start, stop);
        return result != null ? result : Collections.emptySet();
    }

    /** 有序集合 -> ZRANGEBYSCORE：按分数升序区间。示例：ZRANGEBYSCORE rank 0 100。 */
    @Override
    public Set<String> zrangeByScore(String key, double minScore, double maxScore) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<String> result = redisTemplate.opsForZSet().rangeByScore(key, minScore, maxScore);
        return result != null ? result : Collections.emptySet();
    }

    /** 有序集合 -> ZREVRANGEBYSCORE：按分数降序区间。示例：ZREVRANGEBYSCORE rank 100 0。 */
    @Override
    public Set<String> zrevrangeByScore(String key, double maxScore, double minScore) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        Set<String> result = redisTemplate.opsForZSet().reverseRangeByScore(key, minScore, maxScore);
        return result != null ? result : Collections.emptySet();
    }

    /** 有序集合 -> ZRANK：升序排名。示例：ZRANK rank user1。 */
    @Override
    public Long zrank(String key, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        return redisTemplate.opsForZSet().rank(key, member);
    }

    /** 有序集合 -> ZREVRANK：降序排名。示例：ZREVRANK rank user1。 */
    @Override
    public Long zrevrank(String key, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        return redisTemplate.opsForZSet().reverseRank(key, member);
    }

    /** 有序集合 -> ZREMRANGEBYSCORE：按分数区间删除。示例：ZREMRANGEBYSCORE rank 0 50。 */
    @Override
    public Long zremrangeByScore(String key, double scoreMin, double scoreMax) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForZSet().removeRangeByScore(key, scoreMin, scoreMax);
    }

    /** 有序集合 -> ZREMRANGEBYRANK：按索引区间删除。示例：ZREMRANGEBYRANK rank 0 1。 */
    @Override
    public Long zremrangeByRank(String key, long start, long stop) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForZSet().removeRange(key, start, stop);
    }

    /** 有序集合 -> ZREM：删除指定成员。示例：ZREM rank user1 user2。 */
    @Override
    public Long zrem(String key, String... members) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateParams(members, "members");
        return redisTemplate.opsForZSet().remove(key, (Object[]) members);
    }

    /** 有序集合 -> ZSCORE：查询成员分数。示例：ZSCORE rank user1。 */
    @Override
    public Double zscore(String key, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        return redisTemplate.opsForZSet().score(key, member);
    }

    /** 有序集合 -> ZINCRBY：分数自增。示例：ZINCRBY rank 10 user1。 */
    @Override
    public Double zincrby(String key, double increment, String member) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validateKey(member, "member");
        return redisTemplate.opsForZSet().incrementScore(key, member, increment);
    }

    /** 有序集合 -> ZCARD：成员数量。示例：ZCARD rank。 */
    @Override
    public Long zcard(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForZSet().zCard(key);
    }

    /** 有序集合 -> ZCOUNT：按分数区间计数。示例：ZCOUNT rank 0 100。 */
    @Override
    public Long zcount(String key, double scoreMin, double scoreMax) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.opsForZSet().count(key, scoreMin, scoreMax);
    }

    /** 有序集合 -> ZPOPMIN：按分数从小到大弹出成员。示例：ZPOPMIN rank 2。 */
    @Override
    public Set<String> zpopmin(String key, long count) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validatePositive(count, "count");
        Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet().popMin(key, count);
        if (tuples == null || tuples.isEmpty()) {
            return Collections.emptySet();
        }
        return tuples.stream().map(ZSetOperations.TypedTuple::getValue).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** 有序集合 -> ZPOPMAX：按分数从大到小弹出成员。示例：ZPOPMAX rank 2。 */
    @Override
    public Set<String> zpopmax(String key, long count) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validatePositive(count, "count");
        Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet().popMax(key, count);
        if (tuples == null || tuples.isEmpty()) {
            return Collections.emptySet();
        }
        return tuples.stream().map(ZSetOperations.TypedTuple::getValue).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** 有序集合 -> ZINTERSTORE：多个有序集合交集存入目标。示例：ZINTERSTORE dest 2 a b。 */
    @Override
    public Long zinterstore(String destination, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(destination, "destination");
        validateParams(keys, "keys");
        String first = keys[0];
        List<String> others = keys.length > 1 ? Arrays.asList(keys).subList(1, keys.length) : Collections.emptyList();
        return redisTemplate.opsForZSet().intersectAndStore(first, others, destination);
    }

    /** 有序集合 -> ZUNIONSTORE：多个有序集合并集存入目标。示例：ZUNIONSTORE dest 2 a b。 */
    @Override
    public Long zunionstore(String destination, String... keys) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(destination, "destination");
        validateParams(keys, "keys");
        String first = keys[0];
        List<String> others = keys.length > 1 ? Arrays.asList(keys).subList(1, keys.length) : Collections.emptyList();
        return redisTemplate.opsForZSet().unionAndStore(first, others, destination);
    }

    // 区域结束：有序集合

    // 区域：过期控制

    /** 键过期 -> EXPIRE：设置秒级过期。示例：EXPIRE user:1 60。 */
    @Override
    public Boolean expire(String key, int seconds) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validatePositive(seconds, "seconds");
        return redisTemplate.expire(key, Duration.ofSeconds(seconds));
    }

    /** 键过期 -> EXPIREAT：按时间戳过期。示例：EXPIREAT user:1 1700000000。 */
    @Override
    public Boolean expireAt(String key, long timestamp) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        validatePositive(timestamp, "timestamp");
        return redisTemplate.expireAt(key, new Date(timestamp * 1000));
    }

    /** 键过期 -> PERSIST：移除过期时间。示例：PERSIST user:1。 */
    @Override
    public Boolean persist(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.persist(key);
    }

    /** 键过期 -> TTL：查看剩余秒数。示例：TTL user:1。 */
    @Override
    public Long ttl(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        return redisTemplate.getExpire(key);
    }

    // 区域结束

    // 区域：通用

    /** 通用 -> TYPE：查看数据类型。示例：TYPE user:1。 */
    @Override
    public String type(String key) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(key, "key");
        DataType dataType = redisTemplate.type(key);
        return dataType != null ? dataType.code() : "none";
    }

    /** 通用 -> RENAME：强制重命名。示例：RENAME a b。 */
    @Override
    public void rename(String oldKey, String newKey) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(oldKey, "oldKey");
        validateKey(newKey, "newKey");
        redisTemplate.rename(oldKey, newKey);
    }

    /** 通用 -> RENAMENX：目标不存在时重命名。示例：RENAMENX a b。 */
    @Override
    public Boolean renamenx(String oldKey, String newKey) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(oldKey, "oldKey");
        validateKey(newKey, "newKey");
        return redisTemplate.renameIfAbsent(oldKey, newKey);
    }

    /** 通用 -> KEYS：模式匹配列出键（生产慎用）。示例：KEYS user:*。 */
    @Override
    public Set<String> keys(String pattern) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(pattern, "pattern");
        Set<String> result = redisTemplate.keys(pattern);
        return result != null ? result : Collections.emptySet();
    }

    /** 通用 -> SCAN：迭代遍历键。示例：SCAN 0 MATCH user:* COUNT 100。 */
    @Override
    public Cursor<String> scan(String pattern, long count) {
        // 实现思路：
        // 1. 参数校验。
        // 2. 调用 RedisTemplate 执行对应命令。
        validateKey(pattern, "pattern");
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(count).build();
        return redisTemplate.scan(options);
    }

    // 区域结束

    // 区域：扩展工具方法（便于测试或外部访问模板）

    /**
     * 获取底层模板实例
     *
     * 实现逻辑：
     * 1. 返回内部持有的 StringRedisTemplate。
     *
     * @return StringRedisTemplate 实例
     */
    public StringRedisTemplate getRedisTemplate() {
        // 实现思路：
        // 1. 直接返回内部模板对象。
        return redisTemplate;
    }

    /**
     * 简易分布式锁尝试获取。示例：tryLock("lock:a", "token", 5)。
     *
     * 实现逻辑：
     * 1. 校验参数并尝试写入锁。
     * 2. 设置过期时间避免死锁。
     *
     * @param key 锁键
     * @param value 锁值
     * @param expireTime 过期秒数
     * @return 是否获取成功
     */
    public boolean tryLock(String key, String value, int expireTime) {
        // 实现思路：
        // 1. 校验参数并执行加锁。
        validateKey(key, "key");
        validateKey(value, "value");
        validatePositive(expireTime, "expireTime");
        // 核心代码：尝试写入锁
        Boolean success = redisTemplate.opsForValue().setIfAbsent(key, value, Duration.ofSeconds(expireTime));
        return Boolean.TRUE.equals(success);
    }

    /**
     * 释放分布式锁，脚本校验持有者。示例：releaseLock("lock:a", "token")。
     *
     * 实现逻辑：
     * 1. 校验参数并执行 Lua 脚本释放锁。
     * 2. 仅当锁持有者匹配时才删除。
     *
     * @param key 锁键
     * @param value 锁值
     * @return 是否释放成功
     */
    public boolean releaseLock(String key, String value) {
        // 实现思路：
        // 1. 校验参数并执行 Lua 脚本。
        validateKey(key, "key");
        validateKey(value, "value");

        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) " +
                "else return 0 end";

        RedisCallback<Long> callback = connection ->
                connection.eval(
                        script.getBytes(),
                        ReturnType.INTEGER,
                        1,
                        key.getBytes(),
                        value.getBytes()
                );

        // 核心代码：执行脚本释放锁
        Object result = redisTemplate.execute(callback);
        return Long.valueOf(1).equals(result);
    }

    /* ------------------ Pipeline ------------------ */

    @Override
    @SuppressWarnings("unchecked")
    public <R> List<R> executePipelined(org.springframework.data.redis.core.RedisCallback<?> callback) {
        // 【性能调优点：Pipeline 流水线】
        // 适用场景：需要发送大量无依赖关系的指令（如批量写）。
        // 原理：将多条命令在客户端缓冲，一次性发送给服务端，大幅降低网络往返延迟 (RTT)。
        // 注意：Pipeline 模式下，回调函数内部无法立即获取执行结果，所有结果在方法最后统一返回。
        List<Object> results = redisTemplate.executePipelined(callback);
        return (List<R>) results;
    }

    @Override
    public List<String> hmgetPipelined(String key, List<String> fieldList) {
        validateKey(key, "key");
        validateCollection(fieldList, "fieldList");

        // 【性能调优点：Pipeline + HMGET 聚合】
        // 适用场景：典型的 N+1 问题。针对微博详情列表获取，将 N 次查询哈希的操作聚合。
        // 优化：利用 StringRedisConnection 进行强转，可以直接操作字符串，避免底层序列化带来的额外开销。
        List<Object> results = redisTemplate.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) connection -> {
            org.springframework.data.redis.connection.StringRedisConnection stringConn =
                    (org.springframework.data.redis.connection.StringRedisConnection) connection;
            for (String field : fieldList) {
                // 仅仅是发送指令，不等待结果
                stringConn.hGet(key, field);
            }
            return null; // 必须返回 null，真正的结果在 executePipelined 返回收到的 results 列表中
        });

        // 结果类型转换：RedisTemplate 返回的是 Object 列表
        return results.stream()
                .map(obj -> obj != null ? obj.toString() : null)
                .collect(Collectors.toList());
    }
}
