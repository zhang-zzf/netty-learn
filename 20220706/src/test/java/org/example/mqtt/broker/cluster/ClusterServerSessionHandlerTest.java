package org.example.mqtt.broker.cluster;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.broker.node.DefaultServerSessionHandler.HANDLER_NAME;
import static org.example.mqtt.model.ConnAck.ACCEPTED;
import static org.mockito.Mockito.mock;

class ClusterServerSessionHandlerTest {

    public static final String MQTT_CLIENT_A = "mqtt_client_a";
    public static final String MQTT_CLIENT_B = "mqtt_client_b";
    ClusterDbRepo dbRepo = mock(ClusterDbRepo.class);
    Cluster cluster;

    // publish
    Session0 mqttClientA;
    EmbeddedChannel clientA;

    // receive
    Session0 mqttClientB;
    EmbeddedChannel clientB;

    @BeforeEach
    public void beforeEach() {
        ClusterBroker broker = new ClusterBroker(dbRepo);
        cluster = new Cluster().join(broker);
        mqttClientA = new Session0(MQTT_CLIENT_A);
        clientA = createChannel(cluster);
        // clientA 模拟接受 Connect 消息
        clientA.writeInbound(Connect.from(MQTT_CLIENT_A, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(clientA.readOutbound())).isNotNull();
        mqttClientB = new Session0(MQTT_CLIENT_B);
        clientB = createChannel(cluster);
        clientB.writeInbound(Connect.from(MQTT_CLIENT_B, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(clientB.readOutbound())).isNotNull();
    }

    /**
     * cleanSession=1 Connect -> Disconnect
     * <p>Connect 正常流程</p>
     * <p>Disconnect 正常流程</p>
     */
    @Test
    void givenCleanSession1_whenConnectAndDisconnect_thenSuccess() {
        ClusterBroker broker = new ClusterBroker(dbRepo);
        EmbeddedChannel c1 = createChannel(new Cluster().join(broker));
        // Connect
        c1.writeInbound(Connect.from("strReceiver01", (short) 64).toByteBuf());
        then(new ConnAck(c1.readOutbound())).isNotNull()
                .returns((int) ACCEPTED, ConnAck::returnCode)
                .returns(false, ConnAck::sp);
        // Disconnect
        c1.writeInbound(Disconnect.from().toByteBuf());
        then(c1.isActive()).isFalse();
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     */
    @Test
    void givenEmptySession_whenConnectWithCleanSession1_then() {
        ClusterBroker clusterBroker = new ClusterBroker(dbRepo);
        EmbeddedChannel c1 = createChannel(new Cluster().join(clusterBroker));
        c1.writeInbound(Connect.from("strReceiver01", (short) 64).toByteBuf());
        then(new ConnAck(c1.readOutbound())).isNotNull()
                .returns((int) ACCEPTED, ConnAck::returnCode)
                .returns(false, ConnAck::sp)
        ;
    }

    /**
     * <p>订阅 -> 可以接收到消息</p>
     * <p>取消订阅 -> 无法接收到消息</p>
     */
    @Test
    void givenSubscription_whenPublish_thenWillReceivePublish() {
        // given
        // receiver1 subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        List<Subscribe.Subscription> subscriptions = singletonList(new Subscribe.Subscription("t/0", 0));
        clientB.writeInbound(Subscribe.from(pId, subscriptions).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(clientB.readOutbound()).packetIdentifier()).isEqualTo(pId);
        // when
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", mqttClientA.nextPacketIdentifier(),
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        // then
        Publish m = new Publish(clientB.readOutbound());
        then(m).isNotNull()
                .returns((int) qos, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // 取消订阅
        // given
        clientB.writeInbound(Unsubscribe.from(mqttClientB.nextPacketIdentifier(), subscriptions).toByteBuf());
        then(new UnsubAck(clientB.readOutbound())).isNotNull();
        // when
        Publish p2 = Publish.outgoing(false, qos, false, "t/0",
                mqttClientA.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(p2.toByteBuf());
        // then
        then(clientB.<ByteBuf>readOutbound()).isNull();
    }

    /**
     * // given
     * clientB subscribe t/0 (QoS 0)
     * // when
     * clientA publish QoS0 Message to t/0
     * // then
     * clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", mqttClientA.nextPacketIdentifier(),
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns((int) qos, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * clientB subscribe t/0(QoS 0)
     * // when
     * clientA publish QoS1 Message to t/0
     * // then
     * clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS1_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short sendPacketIdentifier = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/0", sendPacketIdentifier,
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        then(new PubAck(clientA.readOutbound())).returns(sendPacketIdentifier, PubAck::packetIdentifier);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(0, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * clientB subscribe t/0 QoS 0
     * // when
     * clientA publish QoS2 Message to t/0
     * // then
     * clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS2_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short sendPacketIdentifier = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/0", sendPacketIdentifier,
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        then(new PubRec(clientA.readOutbound())).returns(sendPacketIdentifier, PubRec::packetIdentifier);
        clientA.writeInbound(PubRel.from(sendPacketIdentifier).toByteBuf());
        then(new PubComp(clientA.readOutbound())).returns(sendPacketIdentifier, PubComp::packetIdentifier);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(0, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * clientB subscribe t/1 (QoS 1)
     * // when
     * clientA publish QoS1 Message to t/1
     * // then
     * clientB receive a QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // clientA 收到 PubAck 消息
        then(new PubAck(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(1, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        clientB.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
    }

    /**
     * // given
     * clientB subscribe t/1 QoS 1
     * // when
     * clientA publish QoS0 Message to t/1
     * // then
     * clientB receive a QoS0 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(0, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * clientB subscribe t/1 QoS 1
     * // when
     * clientA publish QoS2 Message to t/1
     * // then
     * clientB receive a QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS2_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // clientA receive PubRec, should send a PubRel
        then(new PubRec(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        clientA.writeInbound(PubRel.from(publish1PacketId).toByteBuf());
        // clientA receive PubComp
        then(new PubComp(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(1, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // clientB receive QoS1 Message, should send a PubAck
        clientB.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
    }

    /**
     * // given
     * clientB subscribe t/2 QoS 2
     * // when
     * clientA publish QoS2 Message to t/2
     * // then
     * clientB receive a QoS2 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS2_thenReceiver1ReceiveQoS2() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/2", publish1PacketId,
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(2, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubRec 消息
        clientB.writeInbound(PubRec.from(packet.packetIdentifier()).toByteBuf());
        // clientB 收到 PubRel
        then(new PubRel(clientB.readOutbound()).packetIdentifier()).isEqualTo(packet.packetIdentifier());
        // clientB 回复 PubComp
        clientB.writeInbound(PubComp.from(packet.packetIdentifier()).toByteBuf());
        //
        // clientA 收到 PubRec 消息
        then(new PubRec(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        clientA.writeInbound(PubRel.from(publish1PacketId).toByteBuf());
        then(new PubComp(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * // given
     * clientB subscribe t/2 QoS 2
     * // when
     * clientA publish QoS0 Message to t/2
     * // then
     * clientB receive a QoS0 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/2", publish1PacketId,
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(0, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * clientB subscribe t/2 QoS 2
     * // when
     * clientA publish QoS1 Message to t/2
     * // then
     * clientB receive a QoS1 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(clientB.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/2", publish1PacketId,
                Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new Publish(clientB.readOutbound());
        then(packet)
                .returns(1, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        clientB.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
        //
        // clientA 收到 PubAck
        then(new PubAck(clientA.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    private EmbeddedChannel createChannel(Cluster cluster) {
        EmbeddedChannel c = new EmbeddedChannel();
        c.pipeline()
                .addLast(new MqttCodec())
                .addLast(HANDLER_NAME, new ClusterServerSessionHandler(cp -> 0x00, 3, cluster));
        return c;
    }

    public static class Session0 extends AbstractSession {

        protected Session0(String clientIdentifier) {
            super(clientIdentifier);
        }

        @Override
        protected boolean onPublish(Publish packet) {
            return false;
        }

        @Override
        public Set<Subscribe.Subscription> subscriptions() {
            return null;
        }

    }

}