package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.var;
import org.example.mqtt.broker.cluster.ClusterControlPacketContext;
import org.example.mqtt.broker.cluster.ClusterDbQueue;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.example.mqtt.model.Publish;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;

@Disabled
class ClusterDbRepoImplTest {

    static ClusterDbRepoImpl dbRepo;

    @BeforeAll
    public static void beforeAll() {
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        ElasticsearchClient client = config.elasticsearchClient("http://nudocker01:9120", "elastic", "8E78NY1mnfGvQJ6e7aHy");
        dbRepo = new ClusterDbRepoImpl(client);
    }

    @Test
    void given_whenOfferToSessionQueue_then() {
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) 1, byteBuf);
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                clientIdentifier, IN, packet, INIT, null);
        // when
        // first time success
        boolean firstOffer = dbRepo.offerToSessionQueue(null, cpx);
        // then
        then(firstOffer).isTrue();
    }

    @Test
    void given_whenOfferToSessionQueue10Times_then() throws InterruptedException {
        String clientIdentifier = UUID.randomUUID().toString();
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            // when
            // first time success
            boolean firstOffer = dbRepo.offerToSessionQueue(null, cpx);
            // then
            then(firstOffer).isTrue();
            Thread.sleep(2);
        }
    }

    @Test
    void given_whenPeekFromSessionQueue_then() throws InterruptedException {
        // given
        // offer 10 cpx
        String clientIdentifier = UUID.randomUUID().toString();
        String payload = "Hello, World!\n你好，世界。";
        ByteBuf byteBuf = Unpooled.copiedBuffer(payload, UTF_8);
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            // when
            // first time success
            boolean firstOffer = dbRepo.offerToSessionQueue(null, cpx);
            // then
            then(firstOffer).isTrue();
            Thread.sleep(2);
        }
        // when
        List<ClusterControlPacketContext> ccpxList = dbRepo.searchSessionQueue(clientIdentifier,
                ClusterDbQueue.Type.IN_QUEUE, false, 1);
        ClusterControlPacketContext ccpx = ccpxList.get(0);
        // then
        then(ccpx).isNotNull()
                .returns((short) 0, x -> x.packet().packetIdentifier())
                .returns(clientIdentifier, x -> x.clientIdentifier())
                .returns(payload, x -> x.packet().payload().toString(UTF_8))
                .returns(false, x -> x.packet().retain())
                .returns(2, x -> x.packet().qos())
                .returns(false, x -> x.packet().dup())
                .returns("topic/abc/de", x -> x.packet().topicName())
        ;
    }

    /**
     * CAS 原子更新 TopicFilter
     * <p>不存在原子创建</p>
     * <p>注意：此 UT 需要配合 DEBUG 断点，测试 CAS 算法</p>
     * <p>DEBUG case: get -> not exist -> other Thread create the Document -> index failed</p>
     * <p>DEBUG case: get -> exist Document -> other Thread update the Document -> update failed</p>
     * <p>DEBUG case: get -> exist -> other Thread delete the Document -> update failed</p>
     */
    @Test
    void given_whenCasUpdateOrCreateIfNotExist_then() {
        String nodeId = UUID.randomUUID().toString();
        // 第一次添加成功
        dbRepo.addNodeToTopic(nodeId, Arrays.asList("topic/abc"));
        // 第二次添加成功（无需更新DB）
        dbRepo.addNodeToTopic(nodeId, Arrays.asList("topic/abc"));
    }

    /**
     * outQueue is empty
     * <p>空队列 追加消息</p>
     */
    @Test
    void givenEmptyOutQueue_whenOfferToOfflineSession_then() {
        // given
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterServerSession session = ClusterServerSession.from(clientIdentifier, null, null, null);
        dbRepo.saveSession(session);
        // use a shadow copy of the origin Publish
        String payload = "Hello, World!\n你好，世界。";
        ByteBuf byteBuf = Unpooled.copiedBuffer(payload, UTF_8);
        short packetIdentifier = session.nextPacketIdentifier();
        Publish outgoing = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", packetIdentifier, byteBuf);
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo, clientIdentifier, IN, outgoing, INIT, null);
        dbRepo.offerToOutQueueOfTheOfflineSession(session, ccpx);
    }

    /**
     * outQueue is empty
     * <p>非空队列 追加消息</p>
     */
    @Test
    void givenNotEmptyOutQueue_whenOfferToOfflineSession_then() {
        // given
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterServerSession session = ClusterServerSession.from(clientIdentifier, null, null, null);
        dbRepo.saveSession(session);
        // 空队列追加
        ClusterControlPacketContext ccpx = newCcpx(clientIdentifier, session.nextPacketIdentifier());
        dbRepo.offerToOutQueueOfTheOfflineSession(session, ccpx);
        // 非空队列追加
        ClusterServerSession dbSession = dbRepo.getSessionByClientIdentifier(clientIdentifier);
        var ccpx2 = newCcpx(clientIdentifier, dbSession.nextPacketIdentifier());
        dbRepo.offerToOutQueueOfTheOfflineSession(dbSession, ccpx2);
    }

    private ClusterControlPacketContext newCcpx(String clientIdentifier, short packetIdentifier) {
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        Publish outgoing = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", packetIdentifier, byteBuf);
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo, clientIdentifier, IN, outgoing, INIT, null);
        return ccpx;
    }


    @Test
    void givenTopicName_whenMatchTopicFilter_then() {
        Query query = dbRepo.buildTopicMatchQuery("topic/abc/de");
        String queryString = query.toString();
        then(queryString).isEqualTo("Query: {\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"terms\":{\"topicLevel.0\":[\"topic\",\"+\"]}},{\"terms\":{\"topicLevel.1\":[\"abc\",\"+\"]}},{\"terms\":{\"topicLevel.2\":[\"de\",\"+\"]}},{\"bool\":{\"should\":[{\"term\":{\"topicLevel.4\":{\"value\":\"#\"}}},{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"topicLevel.4\"}}]}}]}}]}},{\"bool\":{\"filter\":[{\"terms\":{\"topicLevel.0\":[\"topic\",\"+\"]}},{\"terms\":{\"topicLevel.1\":[\"abc\",\"+\"]}},{\"term\":{\"topicLevel.3\":{\"value\":\"#\"}}}]}},{\"bool\":{\"filter\":[{\"terms\":{\"topicLevel.0\":[\"topic\",\"+\"]}},{\"term\":{\"topicLevel.2\":{\"value\":\"#\"}}}]}},{\"term\":{\"topicLevel.0\":{\"value\":\"#\"}}}]}}]}}");
    }


    @Test
    void givenSessionQueue_whenUpdateCpx_then() {
        String notExistClientIdentifier = UUID.randomUUID().toString();
        ClusterControlPacketContext ccpx = newCcpx(notExistClientIdentifier, (short) 1);
        dbRepo.updateCpxStatus(ccpx);
    }

}