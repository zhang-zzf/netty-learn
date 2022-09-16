package org.example.mqtt.broker.cluster;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.example.mqtt.broker.cluster.infra.es.ClusterDbRepoImpl;
import org.example.mqtt.broker.cluster.infra.es.config.ElasticsearchClientConfig;
import org.example.mqtt.model.Publish;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;

@Disabled
class ClusterDbQueueTest {

    static ClusterDbRepoImpl dbRepo;

    @BeforeAll
    public static void beforeAll() {
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();
        ElasticsearchClient client = config.elasticsearchClient("http://nudocker01:9120", "elastic", "8E78NY1mnfGvQJ6e7aHy");
        dbRepo = new ClusterDbRepoImpl(client);
    }

    @Test
    void given_whenEmptyQueue_then() {
        ClusterDbQueue inQueue = new ClusterDbQueue(dbRepo, "mqtt_3312", ClusterDbQueue.Type.IN_QUEUE);
        then(inQueue.peek()).isNull();
        then(inQueue.poll()).isNull();
    }

    /**
     * 1. offer 10 times
     * 2. new Queue
     * 3. newQueue has 10 item
     */
    @Test
    void given_whenOffer10TimesAndNewQueue_then() {
        String clientIdentifier = "mqtt_3312";
        ClusterDbQueue inQueue = new ClusterDbQueue(dbRepo, clientIdentifier, ClusterDbQueue.Type.IN_QUEUE);
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            inQueue.offer(ccpx);
        }
        inQueue = null;
        // rebuild the queue from DB
        ClusterDbQueue newInQueue = new ClusterDbQueue(dbRepo, clientIdentifier, ClusterDbQueue.Type.IN_QUEUE);
        // offer another 10 item
        for (int i = 10; i < 20; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            newInQueue.offer(ccpx);
        }
        for (int i = 0; i < 20; i++) {
            then(newInQueue.poll()).returns((short) i, c -> c.packetIdentifier());
        }
    }

    /**
     * offer and poll 10 times
     */
    @Test
    void given_whenOfferAndPoll10Times_then() {
        String clientIdentifier = "mqtt_3312";
        ClusterDbQueue inQueue = new ClusterDbQueue(dbRepo, clientIdentifier, ClusterDbQueue.Type.IN_QUEUE);
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            inQueue.offer(ccpx);
        }
        for (int i = 0; i < 10; i++) {
            ClusterControlPacketContext poll = inQueue.poll();
            then(poll).returns((short) i, c -> c.packetIdentifier());
        }
        then(inQueue.poll()).isNull();
    }

    /**
     * offer and poll
     */
    @Test
    void given_whenOfferAndPoll_then() {
        String clientIdentifier = "mqtt_3312";
        ClusterDbQueue inQueue = new ClusterDbQueue(dbRepo, clientIdentifier, ClusterDbQueue.Type.IN_QUEUE);
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) 1, byteBuf);
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo,
                clientIdentifier, IN, packet, INIT, null);
        inQueue.offer(ccpx);
        then(inQueue.poll()).isNotNull();
        then(inQueue.poll()).isNull();
    }

}