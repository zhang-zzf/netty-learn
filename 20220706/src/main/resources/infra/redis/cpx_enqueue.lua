-- request
-- KEYS: {"C:{cId}:S:IN"}
-- ARGV: {"packetIdentifier", "cpxData", "tail.pId"}
-- resp
-- 1->add success; 0-> add failed

local queueKey = KEYS[1]
local pId = ARGV[1]
local cpxData = ARGV[2]
-- @param string, may be nil
local reqTailPId = ARGV[3]
local cpxKey = queueKey .. ":" .. pId

-- 判断 cpx("C:{cId}:S:IN:pId") 是否存在
-- @return number, 0 / 1
if (redis.call('EXISTS', cpxKey) == 1) then
    return 0
end

-- cur tailId
-- @return table(list) {"tail.data"}
local curTailPId = redis.call('LRANGE', queueKey, 0, 0)[1]

-- tailId may be nil
if (reqTailPId) then
    -- 判断当前 queue 的 tail.PId 是否与 Broker 一致
    if (curTailPId ~= reqTailPId) then
        return 0
    end
end

-- store cpx as a hash table
local json = cjson.decode(cpxData)
for k, v in pairs(json) do
    redis.call('HSET', cpxKey, k, v)
end

-- update tail.cpx.next=pId
if (curTailPId) then
    redis.call('HSET', queueKey..":".. curTailPId, 'next', pId)
end

-- enqueue to the tail of the list
-- @return number the length after add to tail of the queue
local queueSize = redis.call('LPUSH', queueKey, pId)
return queueSize;