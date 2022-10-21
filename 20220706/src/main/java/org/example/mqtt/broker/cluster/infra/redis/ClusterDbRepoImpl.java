package org.example.mqtt.broker.cluster.infra.redis;

import com.google.common.io.CharStreams;
import io.micrometer.core.annotation.Timed;
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
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.session.ControlPacketContext;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.springframework.stereotype.Repository;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.example.mqtt.broker.cluster.infra.redis.model.CpxPO.decodePacketIdentifier;
import static org.example.mqtt.broker.cluster.infra.redis.model.CpxPO.encodePacketIdentifier;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;
import static org.redisson.api.RScript.Mode.READ_ONLY;
import static org.redisson.api.RScript.Mode.READ_WRITE;
import static org.redisson.api.RScript.ReturnType.*;

@Slf4j
@Repository
@Timed(histogram = true, percentiles = {0.5, 0.8, 0.9, 0.95, 0.99})
public class ClusterDbRepoImpl implements ClusterDbRepo {

    private static final String LUA_SESSION_QUEUE_DEQUEUE = getStringFromClasspathFile("infra/redis/cpx_dequeue.lua");

    private final RedissonClient redisson;
    private final RScript rScript;
    private final String LUA_MATCH;
    private final String LUA_UNSUBSCRIBE;
    private final String LUA_SUBSCRIBE;
    private final String LUA_CPX_ENQUEUE;
    private final String LUA_CPX_UPDATE;
    private final String LUA_CPX_GET;
    private final String LUA_CPX_SEARCH;
    private final String LUA_CPX_DELETE;

    public ClusterDbRepoImpl(RedissonClient redisson) {
        this.redisson = redisson;
        this.rScript = redisson.getScript(StringCodec.INSTANCE);
        this.LUA_MATCH = getStringFromClasspathFile("infra/redis/match.lua");
        this.LUA_UNSUBSCRIBE = getStringFromClasspathFile("infra/redis/unsubscribe.lua");
        this.LUA_SUBSCRIBE = getStringFromClasspathFile("infra/redis/subscribe.lua");
        this.LUA_CPX_ENQUEUE = getStringFromClasspathFile("infra/redis/cpx_enqueue.lua");
        this.LUA_CPX_UPDATE = getStringFromClasspathFile("infra/redis/cpx_update.lua");
        this.LUA_CPX_GET = getStringFromClasspathFile("infra/redis/cpx_get.lua");
        this.LUA_CPX_SEARCH = getStringFromClasspathFile("infra/redis/cpx_search.lua");
        this.LUA_CPX_DELETE = getStringFromClasspathFile("infra/redis/cpx_delete.lua");
    }

    @Override
    public ClusterServerSession getSession(String clientIdentifier) {
        SessionPO po = redisson
                .<SessionPO>getBucket(toSessionRedisKey(clientIdentifier), SESSION_CODEC)
                .get();
        if (po == null) {
            return null;
        }
        // LPUSH -> enqueue; RPOP -> dequeue
        // redis 视角：List 的队头是 Queue 的队尾。
        // 我们取 Queue 队尾数组
        String pId = redisson
                .<String>getDeque(toCpxQueueRedisKey(clientIdentifier, OUT), StringCodec.INSTANCE)
                .peek();
        po.setOQPId(decodePacketIdentifier(pId));
        return po.toDomain();
    }

    @Override
    public boolean offerCpx(@Nullable ClusterControlPacketContext tail, ClusterControlPacketContext cpx) {
        log.debug("offerCpx req-> {}, {}", tail, cpx);
        String queueRedisKey = toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type());
        CpxPO po = CpxPO.fromDomain(cpx);
        String tailPId = tail == null ? null : encodePacketIdentifier(tail.packetIdentifier());
        log.debug("offerCpx req.redis-> {}, {}, {}", queueRedisKey, po, tailPId);
        Object[] argv = tailPId == null ?
                new Object[]{cpx.pId(), po.jsonEncode()} :
                new Object[]{cpx.pId(), po.jsonEncode(), tailPId};
        // the length of the list after the offer operations
        long curQueueSize = rScript.eval(READ_WRITE, LUA_CPX_ENQUEUE, INTEGER, asList(queueRedisKey), argv);
        log.debug("offerCpx resp-> {}", curQueueSize);
        return curQueueSize > 0;
    }

    @Override
    @Nullable
    public ClusterControlPacketContext getCpx(String clientIdentifier,
                                              ControlPacketContext.Type type,
                                              short packetIdentifier) {
        log.debug("getCpx req-> {}, {}, {}", clientIdentifier, type, packetIdentifier);
        String cpxRedisKey = toCpxRedisKey(toCpxQueueRedisKey(clientIdentifier, type),
                encodePacketIdentifier(packetIdentifier));
        log.debug("getCpx req.redis-> {}", cpxRedisKey);
        // the length of the list after the offer operations
        String json = rScript.eval(READ_WRITE, LUA_CPX_GET, VALUE, asList(cpxRedisKey));
        log.debug("getCpx resp-> {}", json);
        if (json == null) {
            return null;
        }
        return toDomain(CpxPO.jsonDecode(json), clientIdentifier, type);
    }

    private ClusterControlPacketContext toDomain(CpxPO po, String clientIdentifier, ControlPacketContext.Type type) {
        if (po == null) {
            return null;
        }
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(this,
                clientIdentifier, type,
                po.decodePublish(),
                po.decodeStatus(),
                po.decodeNextPacketIdentifier());
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
        // the length of the list after the offer operations
        String json = rScript.eval(READ_WRITE, LUA_CPX_SEARCH, VALUE, asList(queueKey), tail);
        log.debug("searchCpx resp-> {}", json);
        if (json == null) {
            return Collections.emptyList();
        }
        return asList(toDomain(CpxPO.jsonDecode(json), clientIdentifier, type));
    }

    @Override
    public void updateCpxStatus(ClusterControlPacketContext cpx) {
        // 无需并发控制
        log.debug("updateCpxStatus req: {}", cpx);
        CpxPO po = CpxPO.from(cpx.packetIdentifier(), cpx.status());
        String cpxRedisKey = toCpxRedisKey(toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type()), po.getPId());
        log.debug("updateCpxStatus req.redis->key: {}, po: {}", cpxRedisKey, po);
        long num = rScript.eval(READ_WRITE, LUA_CPX_UPDATE, INTEGER, asList(cpxRedisKey), po.jsonEncode());
        log.debug("updateCpxStatus resp.redis: {}", num);
    }

    @Override
    public boolean deleteCpx(ClusterControlPacketContext cpx) {
        log.debug("deleteCpx req-> {}", cpx);
        String queueKey = toCpxQueueRedisKey(cpx.clientIdentifier(), cpx.type());
        log.debug("deleteCpx req.redis-> {}", queueKey);
        long num = rScript.eval(READ_WRITE, LUA_CPX_DELETE, INTEGER,
                asList(queueKey),
                encodePacketIdentifier(cpx.packetIdentifier()));
        log.debug("deleteCpx resp-> {}", num);
        return num > 0;
    }

    @Override
    public void saveSession(ClusterServerSession session) {
        SessionPO po = SessionPO.fromDomain(session);
        String redisKey = toSessionRedisKey(po.getCId());
        RBucket<SessionPO> bucket = redisson.getBucket(redisKey, SESSION_CODEC);
        bucket.set(po);
    }

    private String toCpxQueueRedisKey(String clientIdentifier, ControlPacketContext.Type type) {
        return String.format("C:{%s}:S:%s", clientIdentifier, type.name());
    }

    private String toSessionRedisKey(String clientIdentifier) {
        return String.format("C:{%s}:S", clientIdentifier);
    }

    private static final Codec SESSION_CODEC = JsonJacksonCodec.INSTANCE;

    @Override
    public void deleteSession(ClusterServerSession session) {
        String redisKey = toSessionRedisKey(session.clientIdentifier());
        redisson.getBucket(redisKey, SESSION_CODEC).delete();
    }

    @Override
    public void addNodeToTopic(String nodeId, List<String> tfSet) {
        for (String tf : tfSet) {
            String redisKey = toTopicFilterRedisKey(tf);
            log.debug("addNodeToTopic req-> {}", redisKey, nodeId);
            List<Integer> resp = rScript.eval(READ_WRITE, LUA_SUBSCRIBE, MULTI, asList(redisKey), nodeId);
            log.debug("addNodeToTopic resp-> {}", resp);
        }
    }

    static String toTopicFilterRedisKey(String tf) {
        if (tf.startsWith("/") || tf.endsWith("/")) {
            throw new IllegalArgumentException("TopicFilter(starts with '/' or ends with '/') is not supported.");
        }
        String[] split = tf.split("/");
        split[0] = "{" + split[0] + "}";
        return StringUtils.join(split, "/");
    }

    @Override
    public void removeNodeFromTopic(String nodeId, List<String> tfSet) {
        unsubscribeTopic(nodeId, tfSet, false);
    }

    private void unsubscribeTopic(String nodeId, List<String> tfSet, boolean force) {
        for (String tf : tfSet) {
            String redisKey = toTopicFilterRedisKey(tf);
            log.debug("unsubscribeTopic req-> reddKey:{}, nodeId: {}, force: {}", redisKey, nodeId, force);
            rScript.eval(READ_WRITE, LUA_UNSUBSCRIBE, INTEGER, asList(redisKey), nodeId, force);
        }
    }

    @Override
    public void removeTopic(List<String> tfSet) {
        unsubscribeTopic("", tfSet, true);
    }

    @Override
    public List<ClusterTopic> matchTopic(String topicName) {
        List<ClusterTopic> ret = matchTopicBy(topicName);
        // todo 暂不支持 + 开头的订阅
        // List<ClusterTopic> singleLevelWildCardMatch = matchTopicBy(singleLevelWildCardTopic(topicName));
        // ret.addAll(singleLevelWildCardMatch);
        return ret;
    }

    @Override
    public CompletionStage<List<ClusterTopic>> matchTopicAsync(String topicName) {
        return asyncMatchTopicBy(topicName);
    }

    @NotNull
    private CompletionStage<List<ClusterTopic>> asyncMatchTopicBy(String topicName) {
        // call lua script by sha digest
        log.debug("asyncMatchTopic req-> {}", topicName);
        String redisKey = toTopicFilterRedisKey(topicName);
        RFuture<String> future = rScript.evalAsync(redisKey, READ_ONLY, LUA_MATCH, VALUE, asList(redisKey));
        return future.thenApply(resp -> {
            log.debug("asyncMatchTopic resp-> {}", resp);
            return toClusterTopicDomain(resp);
        });
    }
    private String singleLevelWildCardTopic(String topicName) {
        String[] split = topicName.split("/");
        split[0] = "+";
        return StringUtils.join(split, "/");
    }

    @NotNull
    private List<ClusterTopic> matchTopicBy(String topicName) {
        // call lua script by sha digest
        log.debug("matchTopic req-> {}", topicName);
        String redisKey = toTopicFilterRedisKey(topicName);
        String resp = rScript.eval(redisKey, READ_ONLY, LUA_MATCH, RScript.ReturnType.VALUE, asList(redisKey));
        log.debug("matchTopic resp-> {}", resp);
        return toClusterTopicDomain(resp);
    }

    @NotNull
    private List<ClusterTopic> toClusterTopicDomain(String resp) {
        List<TopicFilterPO> pOList = TopicFilterPO.jsonDecodeArray(resp);
        return pOList.stream()
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

    @Override
    public void removeOfflineSessionFromTopic(String cId, Set<Subscribe.Subscription> subscriptions) {
        log.debug("removeOfflineSessionFromTopic req-> {}, {}", cId, subscriptions);
        for (Subscribe.Subscription sub : subscriptions) {
            String topicFilterRedisKey = toTopicFilterRedisKey(sub.topicFilter());
            String key = toTopicFilterOffRedisKey(topicFilterRedisKey);
            log.debug("removeOfflineSessionFromTopic req.redis-> {}, {}", key, cId);
            redisson.<String, Object>getMap(key, StringCodec.INSTANCE).remove(cId);
        }
    }

    @Override
    public void addOfflineSessionToTopic(String cId, Set<Subscribe.Subscription> subscriptions) {
        log.debug("addOfflineSessionToTopic req-> {}, {}", cId, subscriptions);
        for (Subscribe.Subscription sub : subscriptions) {
            String topicFilterRedisKey = toTopicFilterRedisKey(sub.topicFilter());
            String key = toTopicFilterOffRedisKey(topicFilterRedisKey);
            log.debug("addOfflineSessionToTopic req.redis-> {}, {}, {}", key, cId, sub.qos());
            redisson.<String, Object>getMap(key, StringCodec.INSTANCE).put(cId, sub.qos());
        }
    }

    private String toTopicFilterOffRedisKey(String tf) {
        return tf + ":off";
    }

    @SneakyThrows
    private static String getStringFromClasspathFile(String classpathFileName) {
        InputStream input = ClassLoader.getSystemResourceAsStream(classpathFileName);
        return CharStreams.toString(new InputStreamReader(input, StandardCharsets.UTF_8));
    }

}
