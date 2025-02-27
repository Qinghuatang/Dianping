-- 锁的key
-- local key = KEYS[1]

-- 当前线程标识
-- local threadId = ARGV[1]

-- 获取锁中的线程标识 get key
local id = redis.call("get", KEYS[1])

-- 比较线程标识与锁中的标识是否一致
if id == ARGV[1] then
    -- 一致则释放锁 del key
    return redis.call("del", KEYS[1])
end
return 0