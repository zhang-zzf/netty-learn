package org.example.mqtt.broker.cluster.infra.redis;

import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.mqtt.broker.cluster.*;
import org.example.mqtt.broker.cluster.infra.redis.model.TopicFilterPO;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ClusterDbRepoImpl implements ClusterDbRepo {

    public static final String LUA_SUBSCRIBE = getStringFromClasspathFile("infra/redis/subscribe.lua");
    public static final String LUA_MATCH = getStringFromClasspathFile("infra/redis/match.lua");
    private static final String LUA_UNSUBSCRIBE = getStringFromClasspathFile("infra/redis/unsubscribe.lua");

    private final RedissonClient redisson;

    @Override
    public ClusterServerSession getSessionByClientIdentifier(String clientIdentifier) {
        return null;
    }

    @Override
    public boolean offerToSessionQueue(ClusterControlPacketContext tail, ClusterControlPacketContext cpx) {
        return false;
    }

    @Override
    public List<ClusterControlPacketContext> searchSessionQueue(String clientIdentifier, ClusterDbQueue.Type type, boolean tail, int size) {
        return null;
    }

    @Override
    public void updateCpxStatus(ClusterControlPacketContext cpx) {

    }

    @Override
    public ClusterControlPacketContext getCpxFromSessionQueue(String clientIdentifier, ClusterDbQueue.Type inQueue, short packetIdentifier) {
        return null;
    }

    @Override
    public void deleteFromSessionQueue(ClusterControlPacketContext cpx) {

    }

    @Override
    public void saveSession(ClusterServerSession session) {

    }

    @Override
    public void deleteSession(ClusterServerSession session) {

    }

    @Override
    public Map<String, ClusterTopic> multiGetTopicFilter(List<String> tfSet) {
        return null;
    }

    @Override
    public void addNodeToTopic(String nodeId, List<String> tfSet) {
        for (String tf : tfSet) {
            String redisKey = convertToRedisKey(tf);
            RScript script = redisson.getScript();
            // load lua script into Redis cache to all redis master instances
            String sha1 = script.scriptLoad(LUA_SUBSCRIBE);
            // call lua script by sha digest
            log.debug("addNodeToTopic req-> {}", redisKey, nodeId);
            List<Integer> resp = script.evalSha(RScript.Mode.READ_WRITE, sha1,
                    RScript.ReturnType.MULTI,
                    asList(redisKey), nodeId);
            log.debug("addNodeToTopic resp-> {}", resp);
        }
    }

    String convertToRedisKey(String tf) {
        String[] split = tf.split("/");
        split[0] = "{" + split[0] + "}";
        return StringUtils.join(split, "/");
    }

    @Override
    public void removeNodeFromTopic(String nodeId, List<String> tfSet) {
        for (String tf : tfSet) {
            String redisKey = convertToRedisKey(tf);
            RScript script = redisson.getScript();
            // load lua script into Redis cache to all redis master instances
            String sha1 = script.scriptLoad(LUA_UNSUBSCRIBE);
            // call lua script by sha digest
            log.debug("removeNodeFromTopic req-> {}", redisKey, nodeId);
            script.evalSha(RScript.Mode.READ_WRITE, sha1,
                    RScript.ReturnType.INTEGER,
                    asList(redisKey), nodeId);
        }
    }

    @Override
    public boolean offerToOutQueueOfTheOfflineSession(ClusterServerSession s, ClusterControlPacketContext ccpx) {
        return false;
    }

    @Override
    public List<ClusterTopic> matchTopic(String topicName) {
        List<ClusterTopic> ret = matchTopicBy(topicName);
        List<ClusterTopic> singleLevelWildCardMatch = matchTopicBy(singleLevelWildCardTopic(topicName));
        ret.addAll(singleLevelWildCardMatch);
        return ret;
    }

    private String singleLevelWildCardTopic(String topicName) {
        String[] split = topicName.split("/");
        split[0] = "+";
        return StringUtils.join(split, "/");
    }

    @NotNull
    private List<ClusterTopic> matchTopicBy(String topicName) {
        RScript script = redisson.getScript();
        // load lua script into Redis cache to all redis master instances
        String sha1 = script.scriptLoad(LUA_MATCH);
        // call lua script by sha digest
        log.debug("matchTopic req-> {}", topicName);
        String redisKey = convertToRedisKey(topicName);
        String resp = script.evalSha(RScript.Mode.READ_ONLY, sha1,
                RScript.ReturnType.VALUE, asList(redisKey));
        List<TopicFilterPO> pos = JSON.parseArray(resp, TopicFilterPO.class);
        log.debug("matchTopic resp-> {}", resp);
        return pos.stream()
                .map(po -> po.setValue(convertFromRedisKey(po.getValue())))
                .map(TopicFilterPO::toDomain)
                .collect(toList());
    }

    private String convertFromRedisKey(String key) {
        return key.replace("}", "").substring(1);
    }

    @Override
    public void close() throws IOException {

    }

    @SneakyThrows
    private static String getStringFromClasspathFile(String classpathFileName) {
        final String file = ClassLoader.getSystemResource(classpathFileName).getFile();
        return new String(Files.readAllBytes(new File(file).toPath()), StandardCharsets.UTF_8);
    }

}
