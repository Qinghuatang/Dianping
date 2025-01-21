package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    /**
     * 秒杀券下单
     *
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询秒杀券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);

        // 2.判断秒杀是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }

        // 3.判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }

        // 4.判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        // 锁的名字为订单业务和用户id，表示锁作用在订单业务中的用户级别
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();    // 无参，默认获取失败后不等待，并且超过30秒自动释放锁
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，返回错误信息或重试
            return Result.fail("不允许重复下单");
        }

        try {
            // 需要给createVoucherOrder函数上锁，因为事务要在函数执行完之后才执行
            // 在函数中上锁可能导致函数执行完后有线程切换，发生线程安全问题
            // 但是事务的生效在底层是使用代理对象实现的，而此处如果调用[this.createVoucherOrder]是非代理对象，没有事务功能
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();   // 获取当前对象的代理对象
            return proxy.createVoucherOrder(voucherId);     // 调用代理对象的方法[createVoucherOrder]
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        // 5.1 查询订单
        Long userId = UserHolder.getUser().getId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2 判断订单是否存在
        if (count > 0) {
            // 用户已经购买过该券
            return Result.fail("用户已经购买过一次！");
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                // 使用乐观锁，避免中间有线程切换导致的数据安全问题
                // 一：仅在库存的值等于之前查询到的值时更新，失败率过高
                // 二：仅在库存的值大于0时更新 【注意MySQL数据库本身的update操作有锁，所以不会有问题】
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();

        if (!success) {
            // 扣减失败，一般是由于库存不足
            return Result.fail("库存不足！");
        }

        // 7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 7.2 用户id
        voucherOrder.setUserId(userId);
        // 7.3 代金券id
        voucherOrder.setVoucherId(voucherId);

        save(voucherOrder); // 订单数据写入数据库

        return Result.ok(orderId);
    }
}
