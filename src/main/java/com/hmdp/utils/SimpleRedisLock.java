package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;


public class SimpleRedisLock implements ILock{

    private static final String KEY_PREFIX = "lock:";

    private String name;    // 锁的名字
    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String key = KEY_PREFIX + name;
        // 获取线程标识
        String threadId = String.valueOf(Thread.currentThread().getId());

        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(key, threadId, timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);    // success的值可能是null，这种写法更安全
    }

    @Override
    public void unlock() {
        String key = KEY_PREFIX + name;
        // 释放锁
        stringRedisTemplate.delete(key);
    }
}
