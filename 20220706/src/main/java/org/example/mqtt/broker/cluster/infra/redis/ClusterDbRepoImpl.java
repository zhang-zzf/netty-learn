package org.example.mqtt.broker.cluster.infra.redis;

import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.mqtt.broker.cluster.ClusterControlPacketContext;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.broker.cluster.ClusterTopic;
import org.example.mqtt.broker.cluster.infra.redis.model.CpxPO;
import org.example.mqtt.broker.cluster.infra.redis.model.SessionPO;
import org.example.mqtt.broker.cluster.infra.redis.model.TopicFilterPO;
import org.example.mqtt.session.ControlPacketContext;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RBucket;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Repository;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.example.mqtt.broker.cluster.infra.redis.model.CpxPO.encodePacketIdentifier;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;
import static org.redisson.api.RScript.Mode.READ_ONLY;
import static org.redisson.api.RScript.Mode.READ_WRITE;
import static org.redisson.api.RScript.ReturnType.INTEGER;
import static org.redisson.api.RScript.ReturnType.VALUE;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ClusterDbRepoImpl implements ClusterDbRepo {

    public static final String LUA_SUBSCRIBE = getStringFromClasspathFile("infra/redis/subscribe.lua");
    public static final String LUA_MATCH = getStringFromClasspathFile("infra/redis/match.lua");
    private static final String LUA_UNSUBSCRIBE = getStringFromClasspathFile("infra/redis/unsubscribe.lua");
    private static final String LUA_SESSION_QUEUE_ENQUEUE = getStringFromClasspathFile("infra/redis/cpx_enqueue.lua");
    private static final String LUA_SESSION_QUEUE_DEQUEUE = getStringFromClasspathFile("infra/redis/cpx_dequeue.lua");
    private static final String LUA_CPX_GET = getStringFromClasspathFile("infra/redis/cpx_get.lua");
    private static final String LUA_CPX_SEARCH = getStringFromClasspathFile("infra/redis/cpx_search.lua");
    private static final String LUA_CPX_DELETE = getStringFromClasspathFile("infra/redis/cpx_delete.lua");
    private static final String LUA_CPX_UPDATE = getStringFromClasspathFile("infra/redis/cpx_update.lua");

    private final RedissonClient redisson;

    @Override
    public ClusterServerSession getSession(String clientIdentifier) {
        SessionPO po = redisson
                .<SessionPO>getBucket(toSessionRedisKey(clientIdentifier))
                .get();
        Short outQueuePacketIdentifier = redisson
                .<Short>getDeque(toCpxQueueRedisKey(clientIdentifier, OUT), StringCodec.INSTANCE)
                .peekLast();
        po.setOQPId(outQueuePacketIdentifier);
        return po.toDomain();
    }

    @Override
    public boolean offerCpx(@Nullable ClusterControlPacketContext tail, ClusterControlPacketContext cpx) {
        log.debug("offerCpx req-> {}, {}", tail, cpx);
        String queueRedisKey = toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type());
        CpxPO po = CpxPO.fromDomain(cpx);
        String tailPId = tail == null ? null : encodePacketIdentifier(tail.packetIdentifier());
        log.debug("offerCpx req.redis-> {}, {}, {}", queueRedisKey, po, tailPId);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String sha1 = script.scriptLoad(LUA_SESSION_QUEUE_ENQUEUE);
        // the length of the list after the offer operations
        Integer curQueueSize = script.evalSha(READ_WRITE, sha1, INTEGER,
                asList(queueRedisKey),
                cpx.pId(), po.jsonEncode(), tailPId);
        log.debug("addNodeToTopic resp-> {}", curQueueSize);
        return curQueueSize > 0;
    }

    @Override
    public ClusterControlPacketContext getCpx(String clientIdentifier,
                                              ControlPacketContext.Type type,
                                              short packetIdentifier) {
        log.debug("getCpx req-> {}, {}, {}", clientIdentifier, type, packetIdentifier);
        String cpxRedisKey = toCpxRedisKey(toCpxQueueRedisKey(clientIdentifier, type),
                encodePacketIdentifier(packetIdentifier));
        log.debug("getCpx req.redis-> {}", cpxRedisKey);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String sha1 = script.scriptLoad(LUA_CPX_GET);
        // the length of the list after the offer operations
        String json = script.evalSha(READ_ONLY, sha1, VALUE, asList(cpxRedisKey));
        log.debug("getCpx resp-> {}", json);
        ClusterControlPacketContext ccpx = toDomain(CpxPO.jsonDecode(json), clientIdentifier, type);
        return ccpx;
    }

    private ClusterControlPacketContext toDomain(CpxPO po, String clientIdentifier, ControlPacketContext.Type type) {
        if (po == null) {
            return null;
        }
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(this,
                clientIdentifier, type,
                po.decodePublish(),
                po.decodeStatus(),
                po.decodePacketIdentifier());
        return ccpx;
    }

    private String toCpxRedisKey(String queueKey, String pId) {
        return queueKey + ":" + pId;
    }

    @Override
    public List<ClusterControlPacketContext> searchCpx(String clientIdentifier,
                                                       ControlPacketContext.Type type,
                                                       boolean tail,
                                                       int size) {
        if (size != 1) {
            throw new UnsupportedOperationException();
        }
        log.debug("searchCpx req-> {}, {}, {}, {}", clientIdentifier, type, tail, size);
        String queueKey = toCpxQueueRedisKey(clientIdentifier, type);
        log.debug("searchCpx req.redis-> {}", queueKey);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String sha1 = script.scriptLoad(LUA_CPX_SEARCH);
        // the length of the list after the offer operations
        String json = script.evalSha(READ_ONLY, sha1, VALUE, asList(queueKey), tail);
        log.debug("searchCpx resp-> {}", json);
        List<CpxPO> arr = CpxPO.jsonDecodeArray(json);
        return arr.stream()
                .map(po -> toDomain(po, clientIdentifier, type))
                .collect(toList());
    }

    @Override
    public void updateCpxStatus(ClusterControlPacketContext cpx) {
        // 无需并发控制
        log.debug("updateCpxStatus req: {}", cpx);
        CpxPO po = CpxPO.fromDomain(cpx);
        String cpxRedisKey = toCpxRedisKey(toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type()), po.getPId());
        log.debug("updateCpxStatus req.redis->key: {}, po: {}", cpxRedisKey, po);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String sha1 = script.scriptLoad(LUA_CPX_UPDATE);
        int num = script.evalSha(READ_WRITE, sha1, INTEGER,
                asList(cpxRedisKey),
                po.jsonEncode());
        log.debug("updateCpxStatus resp.redis: {}", num);
    }

    @Override
    public boolean pollCpx(ClusterControlPacketContext cpx) {
        log.debug("deleteCpx req-> {}", cpx);
        String queueKey = toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type());
        log.debug("deleteCpx req.redis-> {}", queueKey);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String sha1 = script.scriptLoad(LUA_CPX_DELETE);
        int num = script.evalSha(READ_WRITE, sha1, INTEGER,
                asList(queueKey),
                encodePacketIdentifier(cpx.packetIdentifier()));
        log.debug("deleteCpx resp-> {}", num);
        return num > 0;
    }

    @Override
    public void saveSession(ClusterServerSession session) {
        SessionPO po = SessionPO.fromDomain(session);
        String redisKey = toSessionRedisKey(po.getCId());
        RBucket<SessionPO> bucket = redisson.getBucket(redisKey);
        bucket.set(po);
    }

    private String toCpxQueueRedisKey(String clientIdentifier, ControlPacketContext.Type type) {
        return String.format("C:{%s}:S:%s", clientIdentifier, type.name());
    }

    private String toSessionRedisKey(String clientIdentifier) {
        return String.format("C:{%s}:S", clientIdentifier);
    }

    @Override
    public void deleteSession(ClusterServerSession session) {
        String redisKey = toSessionRedisKey(session.clientIdentifier());
        redisson.getBucket(redisKey).delete();
    }

    @Override
    public void addNodeToTopic(String nodeId, List<String> tfSet) {
        for (String tf : tfSet) {
            String redisKey = toTopicFilterRedisKey(tf);
            RScript script = redisson.getScript(StringCodec.INSTANCE);
            // load lua script into Redis cache to all redis master instances
            String sha1 = script.scriptLoad(LUA_SUBSCRIBE);
            // call lua script by sha digest
            log.debug("addNodeToTopic req-> {}", redisKey, nodeId);
            List<Integer> resp = script.evalSha(READ_WRITE, sha1,
                    RScript.ReturnType.MULTI,
                    asList(redisKey), nodeId);
            log.debug("addNodeToTopic resp-> {}", resp);
        }
    }

    String toTopicFilterRedisKey(String tf) {
        String[] split = tf.split("/");
        split[0] = "{" + split[0] + "}";
        return StringUtils.join(split, "/");
    }

    @Override
    public void removeNodeFromTopic(String nodeId, List<String> tfSet) {
        for (String tf : tfSet) {
            String redisKey = toTopicFilterRedisKey(tf);
            RScript script = redisson.getScript(StringCodec.INSTANCE);
            // load lua script into Redis cache to all redis master instances
            String sha1 = script.scriptLoad(LUA_UNSUBSCRIBE);
            // call lua script by sha digest
            log.debug("removeNodeFromTopic req-> {}", redisKey, nodeId);
            script.evalSha(READ_WRITE, sha1,
                    INTEGER,
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
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        // load lua script into Redis cache to all redis master instances
        String sha1 = script.scriptLoad(LUA_MATCH);
        // call lua script by sha digest
        log.debug("matchTopic req-> {}", topicName);
        String redisKey = toTopicFilterRedisKey(topicName);
        String resp = script.evalSha(RScript.Mode.READ_ONLY, sha1,
                RScript.ReturnType.VALUE, asList(redisKey));
        List<TopicFilterPO> pos = JSON.parseArray(resp, TopicFilterPO.class);
        log.debug("matchTopic resp-> {}", resp);
        return pos.stream()
                .map(po -> po.setValue(fromTopicFilterRedisKey(po.getValue())))
                .map(TopicFilterPO::toDomain)
                .collect(toList());
    }

    private String fromTopicFilterRedisKey(String key) {
        return key.replace("}", "").substring(1);
    }

    @Override
    public void close() throws IOException {
        redisson.shutdown();
    }

    @SneakyThrows
    private static String getStringFromClasspathFile(String classpathFileName) {
        final String file = ClassLoader.getSystemResource(classpathFileName).getFile();
        return new String(Files.readAllBytes(new File(file).toPath()), StandardCharsets.UTF_8);
    }

}
