-- request
-- KEYS: {"C:{cId}:S:IN"}
-- resp
-- cpx

local queueKey = KEYS[1]

-- dequeue from head
-- @return string, may be nil
local pId = redis.call('RPOP', queueKey)
if (not pId) then
    return "{}"
end
local cpxKey = queueKey .. ":" .. pId
local resp = {}
-- @return table(list)
--1) "pId"
--2) "0x006"
--3) "status"
--4) "INIT"
local data = redis.call('HGETALL', cpxKey)
for i, v in ipairs(data) do
    if (i % 2 == 1) then
        resp[v] = data[i + 1]
    end
end
-- @return number
local removedNum = redis.call('DEL', cpxKey)
if (removedNum == 0) then
    -- toto warn.log
end
return cjson.encode(resp)