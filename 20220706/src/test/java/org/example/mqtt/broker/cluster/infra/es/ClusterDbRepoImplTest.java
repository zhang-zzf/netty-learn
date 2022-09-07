package org.example.mqtt.broker.cluster.infra.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.example.mqtt.broker.cluster.ClusterControlPacketContext;
import org.example.mqtt.broker.cluster.ClusterDbQueue;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.example.mqtt.model.Publish;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;

class ClusterDbRepoImplTest {

    static ClusterDbRepoImpl dbRepo;

    @BeforeAll
    public static void beforeAll() {
        String inetHost = "nudocker01";
        int inetPort = 9120;
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        ElasticsearchClient client = config.elasticsearchClient(inetHost, inetPort);
        ElasticsearchAsyncClient asyncClient = config.elasticsearchAsyncClient(inetHost, inetPort);
        dbRepo = new ClusterDbRepoImpl(client, asyncClient);
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
        List<ClusterControlPacketContext> ccpxList = dbRepo.fetchFromSessionQueue(clientIdentifier,
                ClusterDbQueue.Type.IN_QUEUE, false, 1);
        ClusterControlPacketContext ccpx = ccpxList.get(0);
        // then
        then(ccpx).isNotNull()
                .returns((short) 9, x -> x.packet().packetIdentifier())
                .returns(clientIdentifier, x -> x.clientIdentifier())
                .returns(payload, x -> x.packet().payload().toString(UTF_8))
                .returns(false, x -> x.packet().retain())
                .returns(2, x -> x.packet().qos())
                .returns(false, x -> x.packet().dup())
                .returns("topic/abc/de", x -> x.packet().topicName())
        ;
    }

}