package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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

        String key = CACHE_SHOP_KEY + id;

        // 1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.若redis缓存中存在商铺信息，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        // 3.若redis缓存中不存在商铺信息，则在数据库中查询商铺信息，并添加到redis缓存中
        Shop shop = getById(id);    // 调用 MyBatis Plus 的api

        // 4.不存在，返回错误
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        // 5.存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));

        // 6.返回
        return Result.ok(shop);
    }
}
