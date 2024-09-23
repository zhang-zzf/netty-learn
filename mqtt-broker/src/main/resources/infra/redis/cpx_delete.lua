-- request
-- KEYS: {"C:{cId}:S:IN"}
-- ARGV: {"pId"}
-- resp
-- 1->deleted;0->no delete

local queueKey = KEYS[1]
local pId = ARGV[1]

-- 判断当前 queue 的 head.pId 是否与 pId 一致
-- @return table(list) {"tail.data"}
local headPId = redis.call('RPOP', queueKey)
if (headPId ~= pId) then
    redis.call('RPUSH', headPId)
    return 0
end
-- 删除 cpx
redis.call('DEL', queueKey .. ":" .. pId)
return 1;