package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 服务实现类
 */
@Slf4j
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

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 阻塞队列，存放订单信息
    // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();  // 处理生成订单的线程池

    @PostConstruct  // 在当前类初始化完毕后执行 [因为秒杀相关的业务随时可能发生，需要尽早初始化]
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());   // 线程池执行生成订单的任务
    }

    private class VoucherOrderHandler implements Runnable {  // 执行生成订单的任务

        String queueName = "stream.orders";     // 消息队列的名称

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders > (最近一条未消费的消息)
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();     // 消息队列中存储的键值对集合
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);

                    // 5.ACK确认 SACK stream.orders g1 id     【record.getId() 得到的是消息id】
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 如果在处理消息的过程中发生异常，就没有进行ACK确认，消息就会进入pendingList，就要去pendingList尝试取出消息进行处理
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    // 读取PendingList中的消息不需要阻塞，并且0表示读取pending-list
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        // 如果获取失败，说明pending-list中没有消息，结束循环
                        break;
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();     // 消息队列中存储的键值对集合
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);

                    // 5.ACK确认 SACK stream.orders g1 id     【record.getId() 得到的是消息id】
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    // 如果在处理pending-list中的消息的过程中发生异常，不用递归调用[handlePendingList]，因为会进入下一次循环

                    try {
                        Thread.sleep(20);   // 休眠20毫秒，防止循环执行的频率过高
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    // 使用阻塞队列的代码：
/*    private class VoucherOrderHandler implements Runnable {  // 执行生成订单的任务
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();  // 队列中有元素才取出，否则阻塞，无需担心死循环问题
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户
        Long userId = voucherOrder.getUserId();     // 当前是线程池执行，非主线程，所以不能在UserHolder中获取userId
        // 2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3.获取锁
        // 【注意】：此处不用锁也没问题，因为前面用户抢券时会将抢券成功的用户id记录到Redis中，并使用了Lua脚本保证了原子性，
        // 因此这里不需要加锁保证一人一单功能
        boolean isLock = lock.tryLock();    // 无参，默认获取失败后不等待，并且超过30秒自动释放锁
        // 4.判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，返回错误信息或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);     // 调用代理对象的方法[createVoucherOrder]，注意获取的是主线程的代理对象
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy; // 记录主线程对应的代理对象

    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        long orderId = redisIdWorker.nextId("order");

        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, // 加载的模板对象
                Collections.emptyList(),    // 键参数
                voucherId.toString(), userId.toString(), String.valueOf(orderId) // 值参数
        );

        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.获取代理对象
        this.proxy = (IVoucherOrderService) AopContext.currentProxy();   // 获取当前对象的代理对象

        // 4.返回订单id
        return Result.ok(orderId);
    }

    @Override
/*    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();

        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, // 加载的模板对象
                Collections.emptyList(),    // 键参数
                voucherId.toString(), userId.toString() // 值参数
        );

        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 2.2 为0，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4 用户id
        voucherOrder.setUserId(userId);
        // 2.5 代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6 放入阻塞队列
        orderTasks.add(voucherOrder);

        // 3.获取代理对象
        this.proxy = (IVoucherOrderService) AopContext.currentProxy();   // 获取当前对象的代理对象

        // 4.返回订单id
        return Result.ok(orderId);
    }*/


/*    @Override
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
    }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.一人一单
        // 5.1 查询订单
        Long userId = voucherOrder.getUserId();     // 已修改为异步执行的，因此不能从UserHolder获取用户id
        Long voucherId = voucherOrder.getVoucherId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2 判断订单是否存在
        if (count > 0) {
            // 用户已经购买过该券
            log.error("用户已经购买过一次！");
            return;
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
            log.error("库存不足!");
            return;
        }

        save(voucherOrder); // 订单数据写入数据库
    }
}
