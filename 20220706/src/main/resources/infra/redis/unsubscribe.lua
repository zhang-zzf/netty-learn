-- request
-- unsubscribe topic/abc/de node1
-- KEYS: {{topic}/abc/de}
-- ARGV: node1
-- resp

if (redis.call('SREM', KEYS[1], ARGV[1]) == 0) then
    return ;
end
if (redis.call("EXISTS", KEYS[1]) == 1) then
    -- 从 Topic 移除订阅成功，移除后依旧存在其他订阅者
    return
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
    -- 孩子节点没有子节点且孩子节点没有订阅者
    if (redis.call('EXISTS', childPath) == 0) then
        local childTopicPath = string.gsub(childPath, "_", "")
        if (redis.call('EXISTS', childTopicPath) == 0) then
            redis.call('SREM', parent, child)
            return true;
        end
    end
    -- 子节点不动，向上的所有父节点都不动
    return false;
end
-- function

-- {{topic}/abc/de} -> {{topic}, abc, de}
local topicLevels = split(KEYS[1], "/")
-- {{topic}, abc, de} -> {{topic}_, abc_, de_}
for i, v in ipairs(topicLevels) do
    topicLevels[i] = v .. '_'
end

-- dfs 遍历订阅树
if (redis.call('EXISTS', topicLevels[1]) == 1) then
    dfs(topicLevels, 1, topicLevels[1])
end