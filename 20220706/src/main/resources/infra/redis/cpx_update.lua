-- request
-- KEYS: {"C:{cId}:S:IN:pId"}
-- ARGV: {"{}"}
-- resp
-- 1->updated;0->no update

local cpxKey = KEYS[1]
local cpxData = ARGV[1]
local statusFieldName = 'status'

if (redis.call('EXISTS', cpxKey) == 0) then
    return 0
end

local status = cjson.decode(cpxData)[statusFieldName]
redis.call('HSET', cpxKey, statusFieldName, status)
return 1