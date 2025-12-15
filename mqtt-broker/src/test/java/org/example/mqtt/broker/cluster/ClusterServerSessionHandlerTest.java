package org.example.mqtt.broker.cluster;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.broker.node.DefaultServerSessionHandler.HANDLER_NAME;
import static org.example.mqtt.model.ConnAck.ACCEPTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Disconnect;
import org.example.mqtt.model.PubAck;
import org.example.mqtt.model.PubComp;
import org.example.mqtt.model.PubRec;
import org.example.mqtt.model.PubRel;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.PublishInbound;
import org.example.mqtt.model.SubAck;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.UnsubAck;
import org.example.mqtt.model.Unsubscribe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;

class ClusterServerSessionHandlerTest {

    public static final String MQTT_CLIENT_A = "mqtt_client_a";
    public static final String MQTT_CLIENT_B = "mqtt_client_b";
    ClusterBrokerState dbRepo = mock(ClusterBrokerState.class);
    Cluster cluster;

    // publish
    ServerSession mqttClientA;
    EmbeddedChannel clientA;

    // receive
    ServerSession mqttClientB;
    EmbeddedChannel clientB;

    @BeforeEach
    public void beforeEach() {
        cluster = new Cluster();
        ClusterBroker broker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(broker);
        clientA = createChannel(cluster);
        // clientA 模拟接受 Connect 消息
        clientA.writeInbound(Connect.from(MQTT_CLIENT_A, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(((ConnAck) ControlPacket.from(clientA.readOutbound()))).isNotNull();
        mqttClientA = broker.session(MQTT_CLIENT_A);
        //
        clientB = createChannel(cluster);
        clientB.writeInbound(Connect.from(MQTT_CLIENT_B, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(((ConnAck) ControlPacket.from(clientB.readOutbound()))).isNotNull();
        mqttClientB = broker.session(MQTT_CLIENT_B);
        // topicFilter 匹配自己
        ClusterTopic any = new ClusterTopic("any")
            .setNodes(new HashSet<>() {{
                add(broker.nodeId());
            }})
            .setOfflineSessions(new HashMap<>(0));
        given(dbRepo.matchTopic(any())).willReturn(singletonList(any));
        given(dbRepo.matchTopicAsync(any())).willReturn(new CompletableFuture<>());
        given(dbRepo.addNodeToTopicAsync(any(), any())).willReturn(new CompletableFuture<>());
        given(dbRepo.removeNodeFromTopicAsync(any(), any())).willReturn(new CompletableFuture<>());
    }

    /**
     * cleanSession=1 Connect -> Disconnect
     * <p>Connect 正常流程</p>
     * <p>Disconnect 正常流程</p>
     */
    @Test
    void givenCleanSession1_whenConnectAndDisconnect_thenSuccess() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        EmbeddedChannel c1 = createChannel(cluster);
        // Connect
        c1.writeInbound(Connect.from("strReceiver01", (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // Disconnect
        c1.writeInbound(new Disconnect().toByteBuf());
        then(c1.isActive()).isFalse();
    }

    /**
     * cleanSession1 新建 Session
     * <p>Connect cleanSession=1</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     */
    @Test
    void givenEmptySession_whenConnectWithCleanSession1_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        c1.writeInbound(Connect.from("strReceiver01", (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: exist cleanSession=1 Session</p>
     * <p>关闭 exist Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOnlineCleanSession1_whenConnectWithCleanSession1_then() {
        String clientIdentifier = "strReceiver01";
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(cluster);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: exist cleanSession=0 online Session</p>
     * <p>关闭 exist online Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOnlineCleanSession0_whenConnectWithCleanSession1_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        c1.writeInbound(Connect.from("strReceiver01", false, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
        EmbeddedChannel cc1 = createChannel(cluster);
        cc1.writeInbound(Connect.from("strReceiver01", true, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // 删除 ClusterServerSession
        BDDMockito.then(dbRepo).should().deleteSession(any());
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: exist cleanSession=0 offline Session</p>
     * <p>关闭 exist offline Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOfflineCleanSession0_whenConnectWithCleanSession1_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        String strReceiver01 = "strReceiver01";
        EmbeddedChannel cc1 = createChannel(cluster);
        // mock
        ClusterServerSession offlineSession =
            new ClusterServerSessionImpl(Connect.from(strReceiver01, false, (short) 64), cc1, clusterBroker);
        given(dbRepo.getSession(any())).willReturn(offlineSession);
        cc1.writeInbound(Connect.from(strReceiver01, true, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        BDDMockito.then(dbRepo).should().deleteSession(offlineSession);
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     */
    @Test
    void givenEmptySession_whenConnectWithCleanSession0_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        c1.writeInbound(Connect.from("strReceiver01", false, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
    }

    /**
     * <p>cleanSession=0 Disconnect 后 Broker 保留 Session</p>
     * <p>Connect cleanSession=0</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     * <p>Disconnect</p>
     */
    @Test
    void givenCleanSession0_whenConnectAndDisConnect_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        // Connect
        c1.writeInbound(Connect.from("strReceiver01", false, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // Disconnect
        c1.writeInbound(new Disconnect().toByteBuf());
        then(c1.isActive()).isFalse();
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: cleanSession=1 Session</p>
     * <p>Close Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOnlineCleanSession1_whenConnectWithCleanSession0_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        c1.writeInbound(Connect.from("strReceiver01", true, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
        EmbeddedChannel cc1 = createChannel(cluster);
        cc1.writeInbound(Connect.from("strReceiver01", false, (short) 64).toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: cleanSession=0 online Session</p>
     * <p>Close Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOnlineCleanSession0_whenConnectWithCleanSession0_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        EmbeddedChannel c1 = createChannel(cluster);
        Connect connect = Connect.from("strReceiver01", false, (short) 64);
        c1.writeInbound(connect.toByteBuf());
        then(((ConnAck) ControlPacket.from(c1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
        EmbeddedChannel cc1 = createChannel(cluster);
        // mock
        // todo
        // given(dbRepo.getSession(any())).willReturn(ClusterServerSession.from(connect));
        cc1.writeInbound(connect.toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(true, ConnAck::sp)
        ;
    }

    /**
     * <p>Connect 2 times</p>
     * <p>Channel will be closed by the Broker</p>
     */
    @Test
    void givenConnectedSession_whenConnectAgain_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        Connect connect = Connect.from("strReceiver01", false, (short) 64);
        EmbeddedChannel cc1 = createChannel(cluster);
        // mock
        // todo
        // given(dbRepo.getSession(any())).willReturn(ClusterServerSession.from(connect));
        cc1.writeInbound(connect.toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(true, ConnAck::sp);
        cc1.writeInbound(connect.toByteBuf());
        then(cc1.isActive()).isFalse();
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: cleanSession=0 offline Session</p>
     * <p>Close Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOfflineCleanSession0_whenConnectWithCleanSession0_then() {
        Cluster cluster = new Cluster();
        ClusterBrokerImpl clusterBroker = new ClusterBrokerImpl(dbRepo, new DefaultBroker(), cluster);
        cluster.bind(clusterBroker);
        //
        Connect connect = Connect.from("strReceiver01", false, (short) 64);
        EmbeddedChannel cc1 = createChannel(cluster);
        // mock
        // todo
        // given(dbRepo.getSession(any())).willReturn(ClusterServerSession.from(connect));
        cc1.writeInbound(connect.toByteBuf());
        then(((ConnAck) ControlPacket.from(cc1.readOutbound()))).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(true, ConnAck::sp);
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
        then(((SubAck) ControlPacket.from(clientB.readOutbound())).packetIdentifier()).isEqualTo(pId);
        // when
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", mqttClientA.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        // then
        Publish m = new PublishInbound(clientB.readOutbound());
        then(m).isNotNull()
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // 取消订阅
        // given
        clientB.writeInbound(Unsubscribe.from(mqttClientB.nextPacketIdentifier(), subscriptions).toByteBuf());
        then((UnsubAck) ControlPacket.from(clientB.readOutbound())).isNotNull();
        // when
        Publish p2 = Publish.outgoing(false, qos, false, "t/0",
            mqttClientA.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(p2.toByteBuf());
        // then
        then(clientB.<ByteBuf>readOutbound()).isNull();
    }

    /**
     * // given clientB subscribe t/0 (QoS 0) // when clientA publish QoS0 Message to t/0 // then clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", mqttClientA.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given clientB subscribe t/0(QoS 0) // when clientA publish QoS1 Message to t/0 // then clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS1_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short sendPacketIdentifier = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/0", sendPacketIdentifier,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        then((PubAck) ControlPacket.from(clientA.readOutbound())).returns(sendPacketIdentifier, PubAck::packetIdentifier);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given clientB subscribe t/0 QoS 0 // when clientA publish QoS2 Message to t/0 // then clientB receive a QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS2_thenReceiver1ReceiveQoS0() {
        // clientB subscribe t/0 QoS0
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short sendPacketIdentifier = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/0", sendPacketIdentifier,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        then(((PubRec) ControlPacket.from(clientA.readOutbound()))).returns(sendPacketIdentifier, PubRec::packetIdentifier);
        clientA.writeInbound(new PubRel(sendPacketIdentifier).toByteBuf());
        then(((PubComp) ControlPacket.from(clientA.readOutbound()))).returns(sendPacketIdentifier, PubComp::packetIdentifier);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given clientB subscribe t/1 (QoS 1) // when clientA publish QoS1 Message to t/1 // then clientB receive a QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // clientA 收到 PubAck 消息
        then(((PubAck) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        short packetIdentifier = packet.packetIdentifier();
        clientB.writeInbound(new PubAck(packetIdentifier).toByteBuf());
    }

    /**
     * // given clientB subscribe t/1 QoS 1 // when clientA publish QoS0 Message to t/1 // then clientB receive a QoS0 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given clientB subscribe t/1 QoS 1 // when clientA publish QoS2 Message to t/1 // then clientB receive a QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS2_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // clientA receive PubRec, should send a PubRel
        then(((PubRec) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
        clientA.writeInbound(new PubRel(publish1PacketId).toByteBuf());
        // clientA receive PubComp
        then(((PubComp) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // clientB receive QoS1 Message, should send a PubAck
        short packetIdentifier = packet.packetIdentifier();
        clientB.writeInbound(new PubAck(packetIdentifier).toByteBuf());
    }

    /**
     * // given clientB subscribe t/2 QoS 2 // when clientA publish QoS2 Message to t/2 // then clientB receive a QoS2 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS2_thenReceiver1ReceiveQoS2() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(2, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubRec 消息
        short packetIdentifier1 = packet.packetIdentifier();
        clientB.writeInbound(new PubRec(packetIdentifier1).toByteBuf());
        // clientB 收到 PubRel
        then(((PubRel) ControlPacket.from(clientB.readOutbound())).packetIdentifier()).isEqualTo(packet.packetIdentifier());
        // clientB 回复 PubComp
        short packetIdentifier = packet.packetIdentifier();
        clientB.writeInbound(new PubComp(packetIdentifier).toByteBuf());
        //
        // clientA 收到 PubRec 消息
        then(((PubRec) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
        clientA.writeInbound(new PubRel(publish1PacketId).toByteBuf());
        then(((PubComp) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * // given clientB subscribe t/2 QoS 2 // when clientA publish QoS0 Message to t/2 // then clientB receive a QoS0 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given clientB subscribe t/2 QoS 2 // when clientA publish QoS1 Message to t/2 // then clientB receive a QoS1 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = mqttClientB.nextPacketIdentifier();
        clientB.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = ((SubAck) ControlPacket.from(clientB.readOutbound()));
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // clientA 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = mqttClientA.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        clientA.writeInbound(publish.toByteBuf());
        // Broker forward 后 clientB 接受 Publish 消息
        Publish packet = new PublishInbound(clientB.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        short packetIdentifier = packet.packetIdentifier();
        clientB.writeInbound(new PubAck(packetIdentifier).toByteBuf());
        //
        // clientA 收到 PubAck
        then(((PubAck) ControlPacket.from(clientA.readOutbound())).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    private EmbeddedChannel createChannel(Cluster cluster) {
        EmbeddedChannel c = new EmbeddedChannel();
        c.pipeline()
            .addLast(new MqttCodec())
            .addLast(HANDLER_NAME, new ClusterServerSessionHandler(cp -> 0x00, 3, cluster));
        return c;
    }

}