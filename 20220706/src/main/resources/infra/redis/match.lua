-- request
-- match topicName -> topic/abc/de
-- KEYS: {{topic}/abc/de}
-- ARGV:
-- resp
-- {"{topic}/abc/de": ["node1", "node2"], "{topic}/#": ["node3"]}

-- function
local split = function(str, reps)
    local r = {}
    string.gsub(str, '[^' .. reps .. ']+', function(w)
        table.insert(r, w)
    end)
    return r
end

local function fuzzyMatch(topicLevels, curLevel, parent, topics)
    -- {topic}_/abc_/de_/#_/
    if curLevel == #topicLevels then
        topics[#topics + 1] = parent
        if (redis.call('SISMEMBER', parent, '#_') == 1) then
            topics[#topics + 1] = parent .. '/#_'
        end
        -- 递归必须退出
        return
    end
    if (redis.call('SISMEMBER', parent, topicLevels[curLevel + 1]) == 1) then
        fuzzyMatch(topicLevels, curLevel + 1, parent .. '/' .. topicLevels[curLevel + 1], topics)
    end
    if (redis.call('SISMEMBER', parent, '+_') == 1) then
        fuzzyMatch(topicLevels, curLevel + 1, parent .. '/+_', topics)
    end
    if (redis.call('SISMEMBER', parent, '#_') == 1) then
        topics[#topics + 1] = parent .. '/#_'
    end
end
-- function

-- {{topic}/abc/de} -> {{topic}, abc, de}
local topicLevels = split(KEYS[1], "/")
-- {{topic}, abc, de} -> {{topic}_, abc_, de_}
for i, v in ipairs(topicLevels) do
    topicLevels[i] = v .. '_'
end

-- 不存在 {topic}_ 为根节点的订阅数
local topics = { KEYS[1] }
-- 在订阅树中匹配
if (redis.call('EXISTS', topicLevels[1]) == 1) then
    fuzzyMatch(topicLevels, 1, topicLevels[1], topics)
end
local resp = {}
for i, v in ipairs(topics) do
    -- {topic}_/abc_/de_ -> {topic}/abc/de
    v = string.gsub(v, "_", "")
    local subscriber = redis.call('SMEMBERS', v)
    if (#subscriber > 0) then
        local obj = {}
        obj["value"] = v
        obj["nodes"] = subscriber
        resp[#resp+1] = obj
    end
end
-- should return an array
if (#resp == 0) then
    return "[]"
end
return cjson.encode(resp)