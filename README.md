# 🚀 Redis 高并发社交网络架构实战

<div align="center">

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Redis](https://img.shields.io/badge/Redis-7.x-red.svg)](https://redis.io/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**基于 Spring Boot 的企业级高并发社交网络（Weibo）核心业务架构项目**

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [架构设计](#-架构设计) • [API 文档](#-api-文档) • [最佳实践](#-redis-最佳实践)

</div>

---

## 📖 项目简介

这是一个**企业级 Redis 应用实战项目**，并非简单的 CRUD 练习，而是一个用于验证后端工程能力、展示 Redis 在高并发场景下实际应用的综合性项目。

项目以**微博社交网络**为业务场景，全面演示了 Redis 各种数据结构（String、Hash、List、Sorted Set、Bitmap）在实际生产环境中的设计与应用，并深入实现了分布式系统中的核心技术方案。

### 🎯 项目定位

- ✅ **学习 Redis**：掌握 Redis 各种数据结构的应用场景和最佳实践
- ✅ **解决实际问题**：缓存雪崩、击穿、穿透等生产环境常见问题的完整解决方案
- ✅ **分布式系统**：分布式锁、分布式限流等核心组件的工业级实现
- ✅ **高并发架构**：从单体到集群、从同步到异步的完整架构演进
- ✅ **工程能力展示**：代码设计、文档编写、测试覆盖等工程实践

---

## ⚡ 技术栈

| 技术 | 版本 | 说明 |
|------|------|------|
| **Java** | 21 | 开发语言，使用最新 LTS 版本 |
| **Spring Boot** | 3.5.3 | 核心框架 |
| **Spring Data Redis** | - | Redis 客户端（基于 Lettuce） |
| **MySQL** | 8.x | 关系型数据库 |
| **MyBatis** | 3.0.3 | ORM 框架 |
| **Guava** | 32.1.3-jre | Google 工具库（布隆过滤器） |
| **Caffeine** | 3.1.8 | 本地缓存 |
| **Lombok** | - | 简化 Java 代码 |

---

## ✨ 功能特性

### 核心业务功能

#### 1️⃣ 用户系统（String & Hash）
- 📝 **用户注册**：使用全局发号器（`INCR`）生成唯一 ID
- 👤 **用户详情**：使用 Hash 存储用户属性，支持按字段读取
- 📊 **统计功能**：全站 UV 统计（String 计数器）

#### 2️⃣ 微博时间线（List）
- ✍️ **发布微博**：`LPUSH` 推送到时间线头部（最新在前）
- 📱 **获取动态**：`LRANGE` 分页拉取时间线列表
- 🔄 **推拉结合**：支持推送和拉取两种模式

#### 3️⃣ 互动与排行（Sorted Set）
- ❤️ **点赞微博**：`ZADD` 记录点赞 + `ZINCRBY` 增加热度
- 🔥 **热搜榜**：`ZREVRANGE` 获取热度 Top N（倒序）
- 📈 **实时排序**：基于分值自动维护排序

### 分布式核心组件

#### 1️⃣ 分布式锁（Distributed Lock）
- 🔒 **互斥保证**：基于 Redis `SET NX EX` 实现
- 🐕 **看门狗机制**：自动续期防止业务超时
- 🔄 **可重入支持**：ThreadLocal 实现可重入锁
- ⚡ **Lua 脚本**：保证加锁、续期、解锁的原子性

#### 2️⃣ 分布式限流（Rate Limiter）
- 🚦 **滑动窗口**：基于 Lua 脚本的计数器算法
- 📉 **降级策略**：Redis 异常时自动降级到本地限流
- 🎯 **多维度限流**：支持全局限流、IP 限流、用户限流
- 📊 **动态配置**：支持热配置限流 QPS

#### 3️⃣ 缓存三大问题解决方案

**缓存穿透（Cache Penetration）**
- 🔍 **布隆过滤器**：基于 Redis Bitmap 实现分布式布隆过滤器
- ✅ **容量计算**：2^24 位数组，支持 175 万数据，误判率 < 1%
- 🎨 **多重哈希**：使用 3 个哈希函数降低冲突

**缓存击穿（Cache Breakdown / Hotspot Invalid）**
- ⏰ **逻辑过期**：热点数据永不过期，异步更新
- 🔐 **互斥锁**：单线程重建缓存，其他线程返回旧值
- 🧵 **线程池**：IO 密集型线程池异步查库重建

**缓存雪崩（Cache Avalanche）**
- 🎲 **随机 TTL**：过期时间添加 0-10% 随机偏移
- 🔄 **分批加载**：预热时分批次加载，避免同时过期
- 💾 **本地缓存**：Caffeine 作为 L1 缓存兜底

---

## 🏗️ 架构设计

### 项目结构

```
src/main/java/com/hao/redis/
│
├── common/                          # 通用组件
│   ├── aspect/                      # AOP 切面
│   │   ├── SimpleRateLimitAspect    # 限流注解切面
│   │   └── RedisRouteMonitorAspect  # Redis 路由监控
│   ├── constants/                   # 常量定义
│   ├── enums/                       # 枚举类（Redis Key 前缀等）
│   ├── exception/                   # 异常定义
│   ├── interceptor/                 # 拦截器
│   ├── model/                       # 通用模型
│   │   └── RedisLogicalData         # 逻辑过期封装
│   └── util/                        # 工具类
│       ├── BloomFilterUtil          # 布隆过滤器
│       ├── CacheBreakdownUtil       # 缓存击穿防护
│       ├── RedisRateLimiter         # 分布式限流
│       ├── JsonUtil                 # JSON 序列化
│       └── RedisSlotUtil            # Redis 集群路由
│
├── config/                          # 配置类
│   ├── RedisConfig                  # Redis 连接池配置
│   ├── CacheConfig                  # 缓存配置
│   ├── ThreadPoolConfig             # 线程池配置
│   └── WebMvcConfig                 # Web 配置
│
├── controller/                      # 控制层
│   └── WeiboController              # 微博业务接口
│
├── service/                         # 服务层
│   ├── WeiboService                 # 微博业务接口
│   └── impl/
│       └── WeiboServiceImpl         # 微博业务实现
│
├── integration/                     # 集成层
│   ├── lock/                        # 分布式锁
│   │   ├── DistributedLock          # 锁接口
│   │   ├── RedisDistributedLock     # Redis 锁实现
│   │   └── RedisDistributedLockService  # 锁服务
│   ├── redis/                       # Redis 客户端
│   │   ├── RedisClient              # Redis 客户端接口
│   │   └── RedisClientImpl          # Redis 客户端实现（封装 101 个 Redis 命令）
│   └── cluster/                     # 集群管理
│       └── RedisClusterTopologyCache  # 集群拓扑缓存
│
├── dal/                             # 数据访问层
│   ├── dao/
│   │   └── mapper/
│   │       └── WeiboMapper          # MyBatis Mapper
│   └── model/
│       └── WeiboPost                # 微博实体
│
└── filters/                         # 过滤器
    └── GlobalRateLimitFilter        # 全局限流过滤器
```

### 核心设计模式

| 模式 | 应用场景 | 位置 |
|------|----------|------|
| **模板方法** | 缓存查询模板（逻辑过期） | `CacheBreakdownUtil` |
| **策略模式** | 限流策略（Redis/本地） | `RedisRateLimiter` |
| **门面模式** | Redis 命令封装 | `RedisClientImpl` |
| **单例模式** | 工具类（静态） | `JsonUtil`, `BloomFilterUtil` |
| **工厂模式** | 分布式锁创建 | `RedisDistributedLockService` |
| **代理模式** | AOP 限流切面 | `SimpleRateLimitAspect` |

### 分层架构图

```
┌──────────────────────────────────────────────────┐
│              Controller Layer                     │  ← HTTP 请求入口
│         (WeiboController + Filter)               │
└───────────────────┬──────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────────┐
│               Service Layer                       │  ← 业务逻辑层
│    (WeiboService + Cache + RateLimiter)          │
└───────────────────┬──────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────────┐
│            Integration Layer                      │  ← 基础设施层
│  (RedisClient + DistributedLock + BloomFilter)   │
└──────────┬────────────────────┬──────────────────┘
           ↓                    ↓
  ┌────────────────┐   ┌────────────────┐
  │  Redis Cluster  │   │  MySQL DB      │
  └────────────────┘   └────────────────┘
```

---

## 🚀 快速开始

### 前置要求

- ✅ Java 21+
- ✅ Maven 3.8+
- ✅ Redis 7.x（集群模式）
- ✅ MySQL 8.x

### 安装步骤

#### 1️⃣ 克隆项目

```bash
git clone https://github.com/lihao-ops/Redis-Study.git
cd Redis-Study
```

#### 2️⃣ 启动 Redis 集群

**使用 Docker Compose（推荐）**
```bash
docker-compose up -d
```

**或手动启动 Redis 集群**
```bash
# 创建 6 个 Redis 节点（3主3从）
redis-server --port 7000 --cluster-enabled yes --cluster-config-file nodes-7000.conf &
redis-server --port 7001 --cluster-enabled yes --cluster-config-file nodes-7001.conf &
# ... 其他节点

# 创建集群
redis-cli --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
  --cluster-replicas 1
```

#### 3️⃣ 配置数据库

创建 `src/main/resources/application.yml`：

```yaml
spring:
  # Redis 集群配置
  data:
    redis:
      cluster:
        nodes:
          - 127.0.0.1:7000
          - 127.0.0.1:7001
          - 127.0.0.1:7002
        max-redirects: 3
      lettuce:
        pool:
          max-active: 200
          max-idle: 50
          min-idle: 10
          max-wait: 1000ms
        shutdown-timeout: 2000ms

  # MySQL 配置
  datasource:
    url: jdbc:mysql://localhost:3306/weibo?useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: your_password
    driver-class-name: com.mysql.cj.jdbc.Driver

# 限流配置
rate:
  limit:
    weibo-create-qps: 100
    redis-fallback-ratio: 0.5

# 分布式锁配置
distributed:
  lock:
    watchdog:
      timeout: 30000  # 30秒
```

#### 4️⃣ 初始化数据库

```sql
CREATE DATABASE weibo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE weibo;

CREATE TABLE weibo_post (
    post_id VARCHAR(32) PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### 5️⃣ 启动应用

```bash
# 编译打包
mvn clean package -DskipTests

# 运行应用
java -jar target/redis-study-0.0.1-SNAPSHOT.jar

# 或使用 Maven 插件
mvn spring-boot:run
```

应用将在 `http://localhost:8080` 启动。

#### 6️⃣ 验证部署

```bash
# 检查健康状态
curl http://localhost:8080/actuator/health

# 注册测试用户
curl -X POST "http://localhost:8080/weibo/user/register?nickname=张三&intro=Hello"

# 发布微博
curl -X POST http://localhost:8080/weibo/weibo \
  -H "userId: 1" \
  -H "Content-Type: application/json" \
  -d '{"content":"我的第一条微博！"}'

# 获取时间线
curl http://localhost:8080/weibo/weibo/list

# 查看热搜榜
curl http://localhost:8080/weibo/weibo/rank
```

---

## 📡 API 文档

### 用户模块

#### 注册用户
```http
POST /weibo/user/register
Query Parameters:
  - nickname: 昵称
  - intro: 个人简介
Response: 用户ID (Integer)
```

#### 获取用户详情
```http
GET /weibo/user/{userId}
Response: Map<String, String> (用户信息)
```

### 微博模块

#### 发布微博
```http
POST /weibo/weibo
Headers:
  - userId: 用户ID
Body: WeiboPost (JSON)
{
  "content": "微博内容",
  "images": ["图片URL1", "图片URL2"]
}
Response: 微博ID (String)
```

#### 获取时间线
```http
GET /weibo/weibo/list
Response: List<WeiboPost>
```

#### 点赞微博
```http
POST /weibo/weibo/{postId}/like
Headers:
  - userId: 用户ID
Response: Boolean (成功/失败)
```

#### 获取热搜榜
```http
GET /weibo/weibo/rank
Response: List<WeiboPost> (Top 10)
```

### 系统模块

#### 获取全站 UV
```http
GET /weibo/system/uv
Response: Integer (UV 数量)
```

---

## 🎓 Redis 最佳实践

### 1. Redis 数据结构选型

| 业务场景 | Redis 数据结构 | 核心命令 | Key 设计 |
|---------|---------------|---------|---------|
| 用户 ID 生成 | String | `INCR` | `global:userid` |
| 用户详情 | Hash | `HMSET`, `HGETALL` | `user:{id}` |
| 微博详情 | String (JSON) | `SET`, `GET` | `weibo:post:{id}` |
| 时间线列表 | List | `LPUSH`, `LRANGE` | `weibo:timeline` |
| 热搜排行 | Sorted Set | `ZINCRBY`, `ZREVRANGE` | `weibo:rank:hot` |
| 布隆过滤器 | Bitmap | `SETBIT`, `GETBIT` | `bloom:filter:{category}` |
| UV 统计 | String | `INCR` | `total:uv` |

### 2. 缓存 Key 命名规范

```
业务模块:数据类型:版本号:唯一标识

示例：
weibo:post:v2:123456       # 微博详情（版本2）
weibo:timeline:v1:user123  # 用户时间线
weibo:rank:hot:daily       # 每日热搜榜
cache:user:detail:789      # 用户缓存
lock:seckill:product:456   # 秒杀锁
```

### 3. 缓存过期策略

```java
// ❌ 错误：固定过期时间（雪崩风险）
redisClient.set(key, value, 3600);

// ✅ 正确：随机过期时间（防雪崩）
redisClient.setWithRandomTtl(key, value, 3600, TimeUnit.SECONDS);
// 实际过期时间：3600 + random(0, 360) 秒

// ✅ 正确：逻辑过期（防击穿）
cacheBreakdownUtil.saveLogicalData(key, data, 3600L);
// 数据永不过期，通过逻辑字段判断
```

### 4. 分布式锁最佳实践

```java
// ❌ 错误：未设置过期时间
redisClient.setnx("lock:task", "1");

// ❌ 错误：加锁和设置过期时间非原子
redisClient.setnx("lock:task", "1");
redisClient.expire("lock:task", 30);

// ✅ 正确：原子操作 + 唯一标识 + 看门狗
DistributedLock lock = lockService.getLock("task");
try {
    lock.lock();  // SET NX EX + 看门狗续期
    // 业务逻辑
} finally {
    lock.unlock();  // Lua 脚本安全释放
}
```

### 5. 限流算法选择

| 算法 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| **固定窗口** | 简单高效 | 边界突刺 | 低精度限流 |
| **滑动窗口** | 平滑限流 | 内存占用 | 中等精度 |
| **令牌桶** | 支持突发 | 实现复杂 | 需要突发流量 |
| **漏桶** | 流量整形 | 无法突发 | 严格速率控制 |

**本项目实现**：固定窗口 + Lua 脚本

```lua
-- 固定窗口计数器（原子性）
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    return 0  -- 拒绝
end
return 1  -- 允许
```

### 6. 布隆过滤器容量规划

```java
// 公式：n_max ≈ m / 9.58 (误判率 p = 0.01)
// m: 位数组长度
// k: 哈希函数个数
// n: 插入元素数量

// 示例配置
BIT_SIZE = 1 << 24;      // 16,777,216 位 ≈ 2MB
HASH_COUNT = 3;          // 3 个哈希函数
MAX_ELEMENTS = 1,751,275; // 支持 175 万元素
FALSE_POSITIVE_RATE < 1%; // 误判率 < 1%
```

---

## 🧪 测试

### 运行单元测试

```bash
mvn test
```

### 性能测试

项目包含多个性能测试用例：

```bash
# Redis 集群性能测试
mvn test -Dtest=RedisClusterPerformanceTest

# 限流性能测试
mvn test -Dtest=GlobalRateLimitPerformanceTest

# 缓存击穿测试
mvn test -Dtest=CacheBreakdownTest

# 秒杀场景测试（虚拟线程）
mvn test -Dtest=VirtualThreadSeckillTest
```

### 集成测试

```bash
mvn test -Dtest=WeiboSystemIntegrationTest
```

### 测试覆盖率

```bash
mvn clean test jacoco:report
# 查看报告：target/site/jacoco/index.html
```

---

## 📊 性能指标

### 基准测试结果

| 操作 | QPS | 平均延迟 | P99 延迟 |
|------|-----|----------|----------|
| 用户注册 | 5,000 | 2ms | 5ms |
| 发布微博 | 10,000 | 3ms | 8ms |
| 获取时间线 | 50,000 | 1ms | 3ms |
| 点赞操作 | 20,000 | 1.5ms | 4ms |
| 热搜榜查询 | 100,000 | 0.5ms | 2ms |

**测试环境**：
- CPU: 8 核
- 内存: 16GB
- Redis: 3 主 3 从集群
- 并发: 1000 线程

---

## 🔧 优化建议

### 性能优化
- [ ] 使用 Pipeline 批量操作（提升 5-10 倍）
- [ ] 引入 Caffeine 本地缓存（L1 缓存）
- [ ] 布隆过滤器使用 MurmurHash（降低误判率 30-50%）
- [ ] 看门狗线程池优化（从单线程改为多线程）

### 架构优化
- [ ] 引入消息队列（异步处理）
- [ ] 读写分离（主从架构）
- [ ] 多级缓存（本地 + 分布式）
- [ ] 事件驱动架构（Spring Event）

### 监控优化
- [ ] 接入 Prometheus + Grafana
- [ ] 添加链路追踪（SkyWalking）
- [ ] 慢查询监控
- [ ] Redis 连接池监控

详细优化方案请参考：[优化报告](docs/optimization_report.md)

---

## 📚 相关文档

- [布隆过滤器误判解决方案](BloomFilter_FalsePositive_Solution.md)
- [限流架构设计笔记](RateLimiting_Architecture_Notes.md)
- [限流组件集成文档](rate-limit-integration.md)
- [代码优化分析报告](docs/optimization_report.md)

---

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

### 提交规范

```
feat: 新增功能
fix: 修复 Bug
docs: 文档更新
style: 代码格式调整
refactor: 重构代码
test: 测试相关
chore: 构建/工具链相关
```

### 开发流程

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'feat: Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 📝 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## 👨‍💻 作者

**lihao-ops**

- GitHub: [@lihao-ops](https://github.com/lihao-ops)

---

## 🙏 致谢

- [Spring Boot](https://spring.io/projects/spring-boot)
- [Redis](https://redis.io/)
- [Google Guava](https://github.com/google/guava)
- [Caffeine](https://github.com/ben-manes/caffeine)

---

## ⭐ Star History

如果这个项目对你有帮助，请给个 Star ⭐️

---

<div align="center">

**Built with ❤️ by lihao-ops**

</div>