package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.util.ObjectBuilder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.ResponseException;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.ClusterControlPacketContext;
import org.example.mqtt.broker.cluster.ClusterDbQueue;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.infra.es.model.ControlPacketContextPO;
import org.example.mqtt.model.Publish;
import org.example.mqtt.session.ControlPacketContext;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.example.mqtt.broker.cluster.ClusterControlPacketContext.id;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

@Slf4j
@RequiredArgsConstructor
public class ClusterDbRepoImpl implements ClusterDbRepo {

    private final ElasticsearchClient client;
    private final ElasticsearchAsyncClient asyncClient;

    public static final String SESSION_QUEUE_INDEX = "session_queue";

    @Override
    public ServerSession querySessionByClientIdentifier(String clientIdentifier) {
        return null;
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
    public List<ClusterControlPacketContext> fetchFromSessionQueue(String clientIdentifier,
                                                                   ClusterDbQueue.Type type,
                                                                   boolean tail,
                                                                   int size) {
        log.debug("fetchFromSessionQueue req: {}, {}, {}, {}", clientIdentifier, type, tail, size);
        ControlPacketContext.Type cpxType = (type == ClusterDbQueue.Type.IN_QUEUE) ? IN : OUT;
        Query cQuery = new Query.Builder().term(t -> t.field("clientIdentifier").value(clientIdentifier)).build();
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

    @Override
    public void updateSessionQueueStatus(String clientIdentifier, ControlPacketContext.Type type, String id, ControlPacketContext.Status expect, ControlPacketContext.Status update) {

    }

    @SneakyThrows
    @Override
    public ClusterControlPacketContext findFromSessionQueue(String clientIdentifier,
                                                            ClusterDbQueue.Type type,
                                                            short packetIdentifier) {
        log.debug("findFromSessionQueue req: {}, {}, {}", clientIdentifier, type, packetIdentifier);
        ControlPacketContext.Type cpxType = (type == ClusterDbQueue.Type.IN_QUEUE) ? IN : OUT;
        GetResponse<ControlPacketContextPO> resp = client.get(req -> req.index(SESSION_QUEUE_INDEX)
                        .routing(clientIdentifier)
                        .id(id(clientIdentifier, cpxType, packetIdentifier))
                , ControlPacketContextPO.class);
        ClusterControlPacketContext ret = pOToDomain(resp.source());
        log.debug("findFromSessionQueue resp: {}", ret);
        return ret;
    }

}
