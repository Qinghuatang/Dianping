package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.service.impl.ShopServiceImpl.CACHE_REBUILD_EXECUTOR;
import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {  // 缓存穿透

        String key = keyPrefix + id;

        // 1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.若redis缓存中存在商铺信息，则直接返回
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }

        // 判断命中的是否是空值
        if (json != null) {   // 命中的是空字符串
            // 返回一个错误信息
            return null;
        }

        // 3.若redis缓存中不存在商铺信息，则在数据库中查询商铺信息，并添加到redis缓存中
        R r = dbFallback.apply(id);

        // 4.不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 5.存在，写入redis
        this.set(key, r, time, unit);

        // 6.返回
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {  // 逻辑过期解决缓存击穿

        String key = keyPrefix + id;

        // 1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.若redis缓存中不存在商铺信息，则直接返回 【前提场景是redis缓存中一定有数据，如做活动时访问热点数据】
        if (StrUtil.isBlank(json)) {
            return null;
        }

        // 3.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // RedisData类中的data属性是Object类型的，反序列化后要强转为JSONObject类
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.1 未过期，直接返回店铺信息
            return r;
        }

        // 4.2 已过期，需要缓存重建
        // 5.缓存重建
        // 5.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        // 5.2 判断是否获取锁成功
        if (isLock) {
            // 5.3 获取成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // ※释放锁
                    unLock(lockKey);
                }
            });
        }

        // 5.4 获取失败，返回过期的店铺信息
        return r;
    }


    private boolean tryLock(String key) {
        // 使用redis的 setnx 方式实现互斥锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        // 释放互斥锁
        stringRedisTemplate.delete(key);
    }
}
