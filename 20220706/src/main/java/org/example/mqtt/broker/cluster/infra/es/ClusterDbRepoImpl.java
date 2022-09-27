package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.util.ObjectBuilder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.ResponseException;
import org.example.mqtt.broker.cluster.*;
import org.example.mqtt.broker.cluster.infra.es.model.ControlPacketContextPO;
import org.example.mqtt.broker.cluster.infra.es.model.SessionPO;
import org.example.mqtt.broker.cluster.infra.es.model.TopicFilterPO;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.session.ControlPacketContext;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.*;
import static org.example.mqtt.broker.cluster.ClusterControlPacketContext.id;
import static org.example.mqtt.model.ControlPacket.hexPId;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

@Slf4j
@RequiredArgsConstructor
@Repository
public class ClusterDbRepoImpl implements ClusterDbRepo {

    public static final String CLIENT_IDENTIFIER = "clientIdentifier";
    public static final String TOPIC_FILTER = "topic_filter";
    public static final String SESSION_QUEUE_INDEX = "session_queue";
    private static final String SESSION_INDEX = "session";
    public static final String TOPIC_LEVEL_PATH = "topicLevel.";

    private final ElasticsearchClient client;

    @SneakyThrows
    @Override
    public ClusterServerSession getSessionByClientIdentifier(String clientIdentifier) {
        log.debug("getSessionByClientIdentifier req: {}", clientIdentifier);
        GetResponse<SessionPO> resp = getSessionBy(clientIdentifier);
        ClusterServerSession ret = pOToDomain(resp.source());
        log.debug("getSessionByClientIdentifier resp: {}, {}", clientIdentifier, ret);
        return ret;
    }

    private GetResponse<SessionPO> getSessionBy(String clientIdentifier) throws IOException {
        return client.get(req -> req.index(SESSION_INDEX).id(clientIdentifier), SessionPO.class);
    }

    @Override
    public boolean offerToSessionQueue(ClusterControlPacketContext tail,
                                       ClusterControlPacketContext cpx) {
        log.debug("offerToSessionQueue req: {}, {}", tail, cpx);
        try {
            client.index(req -> req.index(SESSION_QUEUE_INDEX)
                    // 以 clientIdentifier 作为路由策略
                    .routing(cpx.clientIdentifier())
                    .id(cpx.id()).document(newPOFromDomain(cpx))
                    // createIfNotExist
                    .opType(OpType.Create));
            if (tail != null) {
                // 更新 next 指针
                client.update(req -> req.index(SESSION_QUEUE_INDEX)
                                .routing(tail.clientIdentifier())
                                .id(tail.id())
                                .doc(new ControlPacketContextPO().setNextPacketIdentifier(cpx.packetIdentifier())),
                        ControlPacketContextPO.class);
            }
        } catch (ResponseException e) {
            log.info("ControlPacketContext({}) already exists. expectedException: {}", cpx.id(), e);
            return false;
        } catch (IOException e) {
            log.error("unExpectedException.", e);
            return false;
        }
        return true;
    }

    @SneakyThrows
    @Override
    public void deleteFromSessionQueue(ClusterControlPacketContext cpx) {
        log.debug("deleteFromSessionQueue req: {}", cpx);
        DeleteResponse resp = client.delete(req -> req.index(SESSION_QUEUE_INDEX)
                // 以 clientIdentifier 作为路由策略
                .routing(cpx.clientIdentifier())
                .id(cpx.id()));
        if ("NotFound".equals(resp.result().name())) {
            log.info("deleteFromSessionQueue deleteNotExist document: {}", cpx);
        }
    }

    @SneakyThrows
    @Override
    public void saveSession(ClusterServerSession session) {
        client.index(req -> req.index(SESSION_INDEX)
                .id(session.clientIdentifier())
                .document(domainToPO(session)));
    }

    @SneakyThrows
    @Override
    public void deleteSession(ClusterServerSession session) {
        String cId = session.clientIdentifier();
        client.delete(req -> req.index(SESSION_INDEX).id(cId));
        // clean SessionQueue
        client.indices().refresh(req -> req.index(SESSION_QUEUE_INDEX));
        client.deleteByQuery(req -> req.index(SESSION_QUEUE_INDEX)
                .routing(cId)
                .query(q -> q.bool(f -> f.filter(m -> m.match(mt -> mt.field(CLIENT_IDENTIFIER).query(cId)))))
        );
        // client.indices().refresh(req -> req.index(SESSION_QUEUE_INDEX));
    }

    @SneakyThrows
    @Override
    public Map<String, ClusterTopic> multiGetTopicFilter(List<String> ids) {
        Map<String, ClusterTopic> ret = new HashMap<>(ids.size());
        List<String> dbIds = mapToDbId(ids);
        MgetResponse<TopicFilterPO> resp = client.mget(req -> req.index(TOPIC_FILTER).ids(dbIds), TopicFilterPO.class);
        for (MultiGetResponseItem<TopicFilterPO> doc : resp.docs()) {
            TopicFilterPO po;
            if (doc.result() == null || (po = doc.result().source()) == null) {
                continue;
            }
            ret.put(po.getValue(), pOToDomain(po));
        }
        return ret;
    }

    private List<String> mapToDbId(List<String> ids) {
        return ids.stream().map(this::mapToDbId).collect(toList());
    }

    private String mapToDbId(String id) {
        return id.replace("/", "_");
    }

    @Override
    public void addNodeToTopic(String nodeId, List<String> ids) {
        for (String id : ids) {
            String dbId = mapToDbId(id);
            // CAS update or createIfNotExist
            boolean casUpdateOrCreateIfNotExist = false;
            while (!casUpdateOrCreateIfNotExist) {
                try {
                    GetResponse<TopicFilterPO> resp = client.get(r -> r.index(TOPIC_FILTER)
                            .id(dbId).sourceIncludes("nodes"), TopicFilterPO.class);
                    if (resp.found()) {
                        TopicFilterPO po = resp.source();
                        if (po.getNodes().add(nodeId)) {
                            client.update(r -> r.index(TOPIC_FILTER).id(dbId).doc(po)
                                            .ifPrimaryTerm(resp.primaryTerm()).ifSeqNo(resp.seqNo()),
                                    TopicFilterPO.class);
                        }
                    } else {
                        // new Document
                        TopicFilterPO po = new TopicFilterPO(id, nodeId);
                        client.index(r -> r.index(TOPIC_FILTER).id(dbId).document(po).opType(OpType.Create));
                    }
                    casUpdateOrCreateIfNotExist = true;
                } catch (ResponseException | ElasticsearchException e) {
                    // createIfNoteExist failed or CAS update failed will throw ResponseException
                    // CAS update a not exist Document will throw ElasticsearchException
                    log.info("casUpdateOrCreateIfNotExist failed, now retry it", e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void removeNodeFromTopic(String nodeId, List<String> ids) {
        for (String id : ids) {
            String dbId = mapToDbId(id);
            // CAS update or createIfNotExist
            boolean casRemoveNode = false;
            while (!casRemoveNode) {
                try {
                    GetResponse<TopicFilterPO> resp = client.get(r -> r.index(TOPIC_FILTER)
                            .id(dbId).sourceIncludes("nodes"), TopicFilterPO.class);
                    if (resp.found()) {
                        TopicFilterPO po = resp.source();
                        if (po.getNodes().remove(nodeId)) {
                            client.update(r -> r.index(TOPIC_FILTER).id(dbId).doc(po)
                                            .ifPrimaryTerm(resp.primaryTerm()).ifSeqNo(resp.seqNo()),
                                    TopicFilterPO.class);
                        }
                        // todo clean Topic if Topic is empty
                    }
                    casRemoveNode = true;
                } catch (ResponseException | ElasticsearchException e) {
                    log.info("casRemoveNode failed, now retry it", e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @SneakyThrows
    @Override
    public boolean offerToOutQueueOfTheOfflineSession(ClusterServerSession s, ClusterControlPacketContext cpx) {
        if (s == null) {
            return true;
        }
        boolean addToOutQueue = false;
        while (!addToOutQueue) {
            try {
                Short tailPacketIdentifier = s.outQueuePacketIdentifier();
                GetResponse<SessionPO> resp = getSessionBy(cpx.clientIdentifier());
                if (!resp.found()) {
                    log.info("offerToOutQueueOfTheOfflineSession [No Session]: {}", s.clientIdentifier());
                    addToOutQueue = true;
                    break;
                }
                SessionPO sPO = resp.source();
                if (sPO.getNodeId() != null) {
                    log.info("offerToOutQueueOfTheOfflineSession Session bound to Broker(nodeId:{}): {}", s.clientIdentifier(), sPO.getNodeId());
                    break;
                }
                if (sPO.getOutQueuePacketIdentifier() != null && !sPO.getOutQueuePacketIdentifier().equals(tailPacketIdentifier)) {
                    // reBuild the packetIdentifier
                    s.outQueuePacketIdentifier(sPO.getOutQueuePacketIdentifier());
                    cpx.packet().packetIdentifier(s.nextPacketIdentifier());
                }
                // CAS update Session
                sPO.setOutQueuePacketIdentifier(cpx.packetIdentifier());
                // throw ResponseException if CAS failed
                client.index(req -> req.index(SESSION_INDEX).id(s.clientIdentifier()).document(sPO)
                        .ifPrimaryTerm(resp.primaryTerm()).ifSeqNo(resp.seqNo()));
                // throw ResponseException if create by id failed(id conflict)
                client.index(req -> req.index(SESSION_QUEUE_INDEX).routing(cpx.clientIdentifier())
                        .id(cpx.id()).document(newPOFromDomain(cpx))
                        .opType(OpType.Create));
                // 更新 next 指针
                if (tailPacketIdentifier != null) {
                    client.update(req -> req.index(SESSION_QUEUE_INDEX)
                                    .routing(cpx.clientIdentifier())
                                    .id(id(cpx.clientIdentifier(), OUT, tailPacketIdentifier))
                                    .doc(new ControlPacketContextPO().setNextPacketIdentifier(cpx.packetIdentifier())),
                            ControlPacketContextPO.class);
                }
                addToOutQueue = true;
            } catch (ResponseException e) {
                log.info("offerToOutQueueOfTheOfflineSession update failed, now retry it", e);
            }
        }
        return addToOutQueue;
    }

    final FieldValue oneLevelWildcard = FieldValue.of("+");
    final FieldValue multiLevelWildcard = FieldValue.of("#");

    final List<FieldValue> multiLevelWildcardList = asList(FieldValue.of("#"));

    @SneakyThrows
    @Override
    public List<ClusterTopic> matchTopic(String topicName) {
        log.debug("matchTopic req: {}", topicName);
        if (topicName == null) {
            return emptyList();
        }
        Query query = buildTopicMatchQuery(topicName);
        SearchResponse<TopicFilterPO> resp = client.search(r -> r
                        .index(TOPIC_FILTER)
                        .trackTotalHits(t -> t.enabled(true))
                        .query(query),
                TopicFilterPO.class);
        // todo match 10K topic?
        log.debug("matchTopic num: {}", resp.hits().total().value());
        List<ClusterTopic> ret = resp.hits().hits().stream()
                .map(Hit::source)
                .map(this::pOToDomain)
                .collect(toList());
        log.debug("matchTopic resp: {}", ret);
        return ret;
    }

    @Override
    public void close() throws IOException {
        client._transport().close();
    }

    Query buildTopicMatchQuery(String topicName) {
        String[] levels = topicName.split("/");
        List<Query> queryList = new ArrayList<>();
        for (int i = 0; i < levels.length; i++) {
            final int lastLevel = levels.length - i;
            List<Query> list = new ArrayList<>(lastLevel + 1);
            for (int j = 0; j < lastLevel; j++) {
                final int fieldName = j;
                Query levelTerms = new Query.Builder().terms(t -> t
                                .field(TOPIC_LEVEL_PATH + fieldName)
                                .terms(tq -> tq.value(asList(FieldValue.of(levels[fieldName]), oneLevelWildcard))))
                        .build();
                list.add(levelTerms);
            }
            Query last;
            if (lastLevel == levels.length) {
                String lastLevelField = TOPIC_LEVEL_PATH + (lastLevel + 1);
                last = new Query.Builder().bool(b -> b
                        .should(r -> r.term(t -> t.field(lastLevelField).value(multiLevelWildcard)))
                        .should(r -> r.bool(b2 -> b2.mustNot(mn -> mn.exists(e -> e.field(lastLevelField)))))
                ).build();
            } else {
                last = new Query.Builder()
                        .term(t -> t.field(TOPIC_LEVEL_PATH + lastLevel).value(multiLevelWildcard))
                        .build();
            }
            list.add(last);
            queryList.add(new Query.Builder().bool(b -> b.filter(list)).build());
        }
        Query matchAll = new Query.Builder().term(t -> t.field(TOPIC_LEVEL_PATH + "0").value(multiLevelWildcard)).build();
        queryList.add(matchAll);
        Query query = new Query.Builder().bool(b -> b.filter(f -> f.bool(fb -> fb.should(queryList)))).build();
        return query;
    }

    private ClusterTopic pOToDomain(TopicFilterPO po) {
        ClusterTopic ret = new ClusterTopic(po.getValue());
        ret.setNodes(po.getNodes());
        if (po.getOfflineSessions() != null) {
            Map<String, Byte> map = po.getOfflineSessions().stream()
                    .collect(toMap(s -> s.getClientIdentifier(), s -> s.getQos()));
            ret.setOfflineSessions(map);
        } else {
            ret.setOfflineSessions(emptyMap());
        }
        return ret;
    }

    private SessionPO domainToPO(ClusterServerSession session) {
        Set<SessionPO.SubscriptionPO> subscriptions = null;
        if (!session.subscriptions().isEmpty()) {
            subscriptions = session.subscriptions().stream().map(this::domainToPO).collect(toSet());
        }
        SessionPO po = new SessionPO().setClientIdentifier(session.clientIdentifier())
                .setNodeId(session.nodeId())
                .setSubscriptions(subscriptions);
        return po;
    }

    private SessionPO.SubscriptionPO domainToPO(Subscribe.Subscription s) {
        return new SessionPO.SubscriptionPO()
                .setTopicFilter(s.topicFilter())
                .setQos((byte) s.qos());
    }

    private ControlPacketContextPO newPOFromDomain(ClusterControlPacketContext cpx) {
        ControlPacketContextPO po = new ControlPacketContextPO()
                .setId(cpx.id())
                .setClientIdentifier(cpx.clientIdentifier())
                .setType(cpx.type().name())
                .setPacketIdentifier(cpx.packetIdentifier())
                .setStatus(cpx.status().name())
                .setPublish(Base64.encode(cpx.packet().toByteBuf()).toString(UTF_8))
                .setCreatedAt(System.currentTimeMillis())
                .setUpdatedAt(System.currentTimeMillis());
        return po;
    }

    @SneakyThrows
    @Override
    public List<ClusterControlPacketContext> searchSessionQueue(String clientIdentifier,
                                                                ClusterDbQueue.Type type,
                                                                boolean tail,
                                                                int size) {
        log.debug("fetchFromSessionQueue req: {}, {}, {}, {}", clientIdentifier, type, tail, size);
        ControlPacketContext.Type cpxType = (type == ClusterDbQueue.Type.IN_QUEUE) ? IN : OUT;
        Query cQuery = new Query.Builder().term(t -> t.field(CLIENT_IDENTIFIER).value(clientIdentifier)).build();
        Query tQuery = new Query.Builder().term(t -> t.field("type").value(cpxType.name())).build();
        // 查询前必须 refresh index
        client.indices().refresh(req -> req.index(SESSION_QUEUE_INDEX));
        Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>> searchRequest = req -> req
                .index(SESSION_QUEUE_INDEX)
                .routing(clientIdentifier)
                .query(q -> q.bool(b -> b.filter(cQuery, tQuery)))
                .sort(s -> s.field(f -> f.field("createdAt").order(tail ? SortOrder.Desc : SortOrder.Asc)))
                .size(size);
        SearchResponse<ControlPacketContextPO> resp = client.search(searchRequest, ControlPacketContextPO.class);
        List<ClusterControlPacketContext> ret = resp.hits().hits().stream()
                .map(Hit::source)
                .map(this::pOToDomain)
                .collect(toList());
        log.debug("fetchFromSessionQueue resp: {}", ret);
        return ret;
    }

    private ClusterControlPacketContext pOToDomain(ControlPacketContextPO po) {
        if (po == null) {
            return null;
        }
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(this,
                po.getClientIdentifier(),
                ControlPacketContext.Type.valueOf(po.getType()),
                new Publish(Base64.decode(Unpooled.copiedBuffer(po.getPublish(), UTF_8))),
                ControlPacketContext.Status.valueOf(po.getStatus()),
                po.getNextPacketIdentifier());
        return ccpx;
    }

    private ClusterServerSession pOToDomain(SessionPO po) {
        if (po == null) {
            return null;
        }
        Set<Subscribe.Subscription> subscriptions = new HashSet<>();
        Set<SessionPO.SubscriptionPO> sPO = po.getSubscriptions();
        if (sPO != null) {
            subscriptions = sPO.stream()
                    .map(o -> new Subscribe.Subscription(o.getTopicFilter(), o.getQos()))
                    .collect(toSet());
        }
        return ClusterServerSession.from(po.getClientIdentifier(), po.getNodeId(),
                subscriptions, po.getOutQueuePacketIdentifier());
    }

    @SneakyThrows
    @Override
    public void updateCpxStatus(ClusterControlPacketContext cpx) {
        // 无需并发控制
        log.debug("updateCpxStatus req: {}", cpx);
        try {
            client.update(req -> req.index(SESSION_QUEUE_INDEX)
                           // 以 clientIdentifier 作为路由策略
                           .routing(cpx.clientIdentifier())
                           .id(cpx.id())
                           .doc(new ControlPacketContextPO()
                                   .setStatus(cpx.status().name())
                                   .setUpdatedAt(System.currentTimeMillis())),
                   ClusterControlPacketContext.class
           );
        } catch (ElasticsearchException e) {
            log.error("updateCpxStatus update a not existed cpx", e);
        }
    }

    @SneakyThrows
    @Override
    public ClusterControlPacketContext getCpxFromSessionQueue(String clientIdentifier,
                                                              ClusterDbQueue.Type type,
                                                              short packetIdentifier) {
        log.debug("getCpxFromSessionQueue req: {}, {}, {}", clientIdentifier, type, hexPId(packetIdentifier));
        ControlPacketContext.Type cpxType = (type == ClusterDbQueue.Type.IN_QUEUE) ? IN : OUT;
        GetResponse<ControlPacketContextPO> resp = client.get(req -> req.index(SESSION_QUEUE_INDEX)
                        .routing(clientIdentifier)
                        .id(id(clientIdentifier, cpxType, packetIdentifier))
                , ControlPacketContextPO.class);
        ClusterControlPacketContext ret = pOToDomain(resp.source());
        log.debug("getCpxFromSessionQueue resp: {}", ret);
        return ret;
    }

}
