package com.hao.redis.report;

import com.hao.redis.integration.redis.RedisClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class RedisClientTest {

    // 假设你在实现类中注入的泛型是 String (如果是 Object 请自行替换)
    @Autowired
    private RedisClient<String> redisClient;

    @Test
    @DisplayName("实验：复现 Lettuce 单连接 TCP 队头阻塞")
    public void testTcpHeadOfLineBlocking() throws InterruptedException {
        System.out.println("====== 开始 TCP 队头阻塞压测实验 ======");

        // 线程 A：扮演“重型卡车”，去拉取那 5MB 的超级大包
        Thread heavyThread = new Thread(() -> {
            System.out.println("【线程 A】(重型) 开始拉取 5MB 大 Key...");
            long start = System.currentTimeMillis();

            // 使用你的 redisClient 或者注入 StringRedisTemplate 都可以
            redisClient.get("{b}massive_key");

            long cost = System.currentTimeMillis() - start;
            System.out.println("【线程 A】(重型) 大 Key 拉取完毕！耗时: " + cost + " ms");
        });

        // 线程 B：扮演“轻量跑车”，去拉取只有几个字节的小包
        Thread lightThread = new Thread(() -> {
            System.out.println("【线程 B】(轻量) 开始拉取小 Key...");
            long start = System.currentTimeMillis();

            redisClient.get("{b}tiny_key");

            long cost = System.currentTimeMillis() - start;
            System.out.println("【线程 B】(轻量) 小 Key 拉取完毕！耗时: " + cost + " ms <--- 【注意看这个时间！】");
        });

        // 1. 先让重型卡车上路，占用唯一的 TCP 通道
        heavyThread.start();

        // 2. 故意让主线程等 5 毫秒，确保大包命令已经发给了 Redis，底层开始疯狂传输
        Thread.sleep(5);

        // 3. 让轻量跑车上路，测试它会不会被堵死
        lightThread.start();

        // 等待两个线程都执行完，防止 JUnit 提前退出
        heavyThread.join();
        lightThread.join();

        System.out.println("====== 实验结束 ======");
    }
}