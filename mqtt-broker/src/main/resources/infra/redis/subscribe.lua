-- request
-- topic/abc/de <-> node1
-- KEYS: {{topic}/abc/de}
-- ARGV: {node1}
-- resp
-- [num1,num2]
-- num1 topic/abc/de 是否存在。1-存在；0-不存在
-- num2 node1 是否已经是 topic/abc/de 的订阅者。 1-不是；0-是

local split = function(str, reps)
    local r = {}
    string.gsub(str, '[^' .. reps .. ']+', function(w)
        table.insert(r, w)
    end)
    return r
end

local topicFilter = KEYS[1]
local subscriber = ARGV[1]

-- 判断 {topic}/abc/de 是否存在
-- num=0/1
if redis.call('EXISTS', topicFilter) == 1 then
    -- TopicFilter 已经存在
    local addResp = redis.call('SADD', topicFilter, subscriber)
    return {1, addResp}
end
-- TopicFilter 不存在. 添加 TopicFilter 并更新订阅树
redis.call('SADD', topicFilter, subscriber)

-- 更新订阅树
-- {topic}_         -> ["abc_"]
-- {topic}_abc_     -> ["de_"]

-- {{topic}, abc, de}
local topicLevels = split(KEYS[1], "/")
-- {{topic}_, abc_, de_}
for i, v in ipairs(topicLevels) do
    topicLevels[i] = v .. '_'
end
local redisKey = topicLevels[1]
for i = 1, #topicLevels - 1 do
    local child = topicLevels[i + 1]
    redis.call('SADD', redisKey, child)
    redisKey = redisKey .. '/' .. child
end
return {0, 1}

