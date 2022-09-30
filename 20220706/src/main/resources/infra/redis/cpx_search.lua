-- request
-- KEYS: {"C:{cId}:S:IN"}
-- ARGV: {false}
-- resp
-- json of array of cpx.data

local queueKey = KEYS[1]
local fromTail = (ARGV[1] == "true")

local pId
if (fromTail) then
    -- fetch from tail
    -- @return table(list), example: {"e1", "e2", "e3"}
    -- LRANGE will return an empty list(table) even if queueKey is not exists
    pId = redis.call('LRANGE', queueKey, 0, 0)[1]
else
    -- fetch from tail
    -- @return table(list), example: {"e1", "e2", "e3"}
    -- LRANGE will return an empty list(table) even if queueKey is not exists
    pId = redis.call('LRANGE', queueKey, -1, -1)[1]
end

local resp = {}
if (pId) then
    -- @return table(list)
    --1) "pId"
    --2) "0x006"
    --3) "status"
    --4) "INIT"
    --5) "next"
    --6) "0x008"
    local data = redis.call('HGETALL', queueKey .. ":" .. pId)
    for i, v in ipairs(data) do
        if (i % 2 == 1) then
            resp[v] = data[i + 1]
        end
    end
end
return cjson.encode(resp)