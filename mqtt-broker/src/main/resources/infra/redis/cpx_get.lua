-- request
-- KEYS: {"C:{cId}:S:IN:pId"}
-- ARGV: {}
-- resp
-- json of cpx.data

local cpxKey = KEYS[1]

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
local json = cjson.encode(resp)
if (json == "{}") then
    return nil
else
    return json
end