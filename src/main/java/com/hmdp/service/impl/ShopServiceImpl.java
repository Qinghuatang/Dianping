package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 根据id查询商铺信息
     *
     * @param id
     * @return
     */
    @Override
    public Result queryShopById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    public Shop queryWithPassThrough(Long id){  // 缓存穿透
        String key = CACHE_SHOP_KEY + id;

        // 1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.若redis缓存中存在商铺信息，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // 判断命中的是否是空值
        if(shopJson != null){   // 命中的是空字符串
            // 返回一个错误信息
            return null;
        }

        // 3.若redis缓存中不存在商铺信息，则在数据库中查询商铺信息，并添加到redis缓存中
        Shop shop = getById(id);    // 调用 MyBatis Plus 的api

        // 4.不存在，返回错误
        if (shop == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 5.存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 6.返回
        return shop;
    }

    public Shop queryWithMutex(Long id){  // 互斥锁解决缓存击穿
        String key = CACHE_SHOP_KEY + id;

        // 1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.若redis缓存中存在商铺信息，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 判断命中的是否是空值
        if(shopJson != null){   // 命中的是空字符串[穿透的结果]
            // 返回一个错误信息
            return null;
        }
        // 3.实现缓存重建
        // 3.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;

        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 3.2 判断是否获取成功
            if(!isLock){
                // 3.3 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            // 3.4 成功，在数据库中查询商铺信息
            shop = getById(id); // 调用 MyBatis Plus 的api
            // 模拟重建的延时
            Thread.sleep(200);

            // 4.不存在，返回错误
            if (shop == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }

            // 5.存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.释放互斥锁
            unLock(lockKey);
        }

        // 7.返回
        return shop;
    }

    private boolean tryLock(String key){
        // 使用redis的 setnx 方式实现互斥锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        // 释放互斥锁
        stringRedisTemplate.delete(key);
    }


    @Override
    @Transactional
    public Result updateShop(Shop shop) {

        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        // 1.更新数据库
        updateById(shop);

        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
