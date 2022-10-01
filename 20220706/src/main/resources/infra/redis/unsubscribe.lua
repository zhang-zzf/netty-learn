-- request
-- unsubscribe topic/abc/de node1
-- KEYS: {"{topic}/abc/de"}
-- ARGV: {"node1", "forceRemoveTopic"}
-- resp
local topicKey = KEYS[1]
local force = (ARGV[2] == "true")

if (not force) then
    -- @return number 0/1
    if (redis.call('SREM', topicKey, ARGV[1]) == 0) then
        return
    end
    -- @return number 0/1
    if (redis.call("EXISTS", topicKey) == 1) then
        -- 从 Topic 移除订阅成功，移除后依旧存在其他订阅者
        return
    end
else
    -- 强制删除 topic
    redis.call('DEL', topicKey)
    redis.call('DEL', topicKey .. ":off")
end


-- Topic 删除成功，更新订阅树
-- function
local split = function(str, reps)
    local r = {}
    string.gsub(str, '[^' .. reps .. ']+', function(w)
        table.insert(r, w)
    end)
    return r
end

local function dfs(topicLevels, curLevel, parent)
    -- {topic}_/abc_/de_/#_/
    if curLevel == #topicLevels then
        return true
    end
    local child = topicLevels[curLevel + 1]
    local childPath = parent .. '/' .. child
    local mayNeedToRemove = false
    if (redis.call('SISMEMBER', parent, child) == 1) then
        mayNeedToRemove = dfs(topicLevels, curLevel + 1, childPath)
    end
    if (not mayNeedToRemove) then
        -- 子节点不动，向上的所有父节点都不动
        return false;
    end
    -- 孩子节点
    -- 1. 没有子节点
    -- 2. 没有订阅者（集群路由信息）
    -- 3. 没有离线订阅者（cleanSession0 离线订阅）
    if (redis.call('EXISTS', childPath) == 0) then
        local childTopicPath = string.gsub(childPath, "_", "")
        if (redis.call('EXISTS', childTopicPath) == 0
                and redis.call('EXISTS', childTopicPath .. ":off") == 0) then
            redis.call('SREM', parent, child)
            return true;
        end
    end
    -- 子节点不动，向上的所有父节点都不动
    return false;
end
-- function

-- {{topic}/abc/de} -> {{topic}, abc, de}
local topicLevels = split(topicKey, "/")
-- {{topic}, abc, de} -> {{topic}_, abc_, de_}
for i, v in ipairs(topicLevels) do
    topicLevels[i] = v .. '_'
end

-- dfs 遍历订阅树
if (redis.call('EXISTS', topicLevels[1]) == 1) then
    dfs(topicLevels, 1, topicLevels[1])
end