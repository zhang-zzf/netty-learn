package org.example.mqtt.broker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.model.ConnAck.ACCEPTED;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.UUID;
import org.example.mqtt.broker.codec.MqttCodec;
import org.example.mqtt.broker.node.DefaultBroker;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Disconnect;
import org.example.mqtt.model.PubAck;
import org.example.mqtt.model.PubComp;
import org.example.mqtt.model.PubRec;
import org.example.mqtt.model.PubRel;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.SubAck;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.UnsubAck;
import org.example.mqtt.model.Unsubscribe;
import org.example.mqtt.session.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServerSessionHandlerTest {

    Broker broker;
    // receiver1
    EmbeddedChannel receiver1;
    ServerSession sReceiver1;

    // publish1
    EmbeddedChannel publish1;
    ServerSession sPublish1;

    @BeforeEach
    public void beforeEach() {
        // 每个 UT 一个新的 Broker
        this.broker = createBroker();
        final String strReceiver01 = "receiver1";
        this.receiver1 = createChannel(this.broker);
        // receiver 模拟接受 Connect 消息
        this.receiver1.writeInbound(Connect.from(strReceiver01, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(this.receiver1.readOutbound())).isNotNull();
        // get Session from Broker
        this.sReceiver1 = this.broker.session(strReceiver01);
        //
        this.publish1 = createChannel(this.broker);
        // publish1 模拟接受 Connect 消息
        String strPublisher01 = "publish1";
        this.publish1.writeInbound(Connect.from(strPublisher01, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(this.publish1.readOutbound())).isNotNull();
        // get Session from Broker
        this.sPublish1 = this.broker.session(strPublisher01);
    }

    private EmbeddedChannel createChannel(Broker broker) {
        EmbeddedChannel c = new EmbeddedChannel();
        c.pipeline().addLast(new MqttCodec())
            .addLast(DefaultServerSessionHandler.HANDLER_NAME,
                new DefaultServerSessionHandler(broker, packet -> 0x00, 3));
        return c;
    }

    private DefaultBroker createBroker() {
        return new DefaultBroker();
    }


    @Test
    void givenInboundPublish_whenReceived_thenItWillBeReleased() {
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        // 需要手动 debug
        publish1.writeInbound(publish.toByteBuf());
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     */
    @Test
    void givenEmptySession_whenConnectWithCleanSession1_then() {
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from("strReceiver01", (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
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
    void givenExistCleanSession1_whenConnectWithCleanSession1_then() {
        final Broker broker = createBroker();
        String clientIdentifier = "strReceiver01";
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
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
        final Broker broker = createBroker();
        String clientIdentifier = "strReceiver01";
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        // cleanSession=0
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, true, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
    }

    /**
     * <p>Connect cleanSession=1</p>
     * <p>Broker: exist cleanSession=0 offline Session</p>
     * <p>关闭 exist offline Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOfflineCleanSession0_whenConnectWithCleanSession1_then() {
        final Broker broker = createBroker();
        String clientIdentifier = "strReceiver01";
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        // cleanSession=0
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // Disconnect
        c1.writeInbound(Disconnect.from().toByteBuf());
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, true, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: no Session</p>
     * <p>创建 new Session</p>
     */
    @Test
    void givenEmptySession_whenConnectWithCleanSession0_then() {
        String clientIdentifier = "strReceiver01";
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        then(broker.session(clientIdentifier)).isNotNull()
            .returns(false, Session::cleanSession);
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
        // given
        String clientIdentifier = "strReceiver01";
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // Connect
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        new ConnAck(c1.readOutbound());
        // Disconnect
        c1.writeInbound(Disconnect.from().toByteBuf());
        // Channel was closed
        then(c1.isActive()).isFalse();
        // 依旧可以从 Broker 中获取 Session 信息
        then(broker.session(clientIdentifier)).isNotNull()
            .returns(false, Session::cleanSession)
            .returns(false, ServerSession::isActive)
        ;
        // 再次 Connect
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
            .returns(true, ConnAck::sp);
        // 从 Broker 中获取 Session 信息
        then(broker.session(clientIdentifier)).isNotNull()
            .returns(false, Session::cleanSession)
            // 再次上线
            .returns(true, Session::isActive);
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: cleanSession=1 Session</p>
     * <p>Close Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOnlineCleanSession1_whenConnectWithCleanSession0_then() {
        String clientIdentifier = "strReceiver01";
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, true, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
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
        String clientIdentifier = "strReceiver01";
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp)
        ;
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(true, ConnAck::sp);
    }

    /**
     * <p>Connect 2 times</p>
     * <p>Channel will be closed by the Broker</p>
     */
    @Test
    void givenConnectedSession_whenConnectAgain_then() {
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        Connect connect = Connect.from("strReceiver01", (short) 64);
        c1.writeInbound(connect.toByteBuf());
        // 读出 ConnAck 消息
        new ConnAck(c1.readOutbound());
        c1.writeInbound(connect.toByteBuf());
        // Channel was closed.
        then(c1.isActive()).isFalse();
    }

    /**
     * <p>Connect cleanSession=0</p>
     * <p>Broker: cleanSession=0 offline Session</p>
     * <p>Close Session -> 创建 new Session</p>
     */
    @Test
    void givenExistOfflineCleanSession0_whenConnectWithCleanSession0_then() {
        String clientIdentifier = "strReceiver01";
        final Broker broker = createBroker();
        EmbeddedChannel c1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        c1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(c1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(false, ConnAck::sp);
        // Disconnect
        c1.writeInbound(Disconnect.from().toByteBuf());
        // cc1 same clientIdentifier with c1
        // that is the same client
        EmbeddedChannel cc1 = createChannel(broker);
        // receiver 模拟接受 Connect 消息
        cc1.writeInbound(Connect.from(clientIdentifier, false, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(cc1.readOutbound())).isNotNull()
            .returns((int) ACCEPTED, ConnAck::returnCode)
            .returns(true, ConnAck::sp);
    }

    /**
     * QoS0 消息不进入 inQueue / outQueue 队列
     * <p>Broker(as Receiver) 接受 QoS0 Publish 不进入 Session.inQueue</p>
     * <p>Broker(as Sender) forward QoS0 Publish to receiver1 不进入 Session.outQueue</p>
     * <p>观察 DEBUG 日志吧，没想到好办法自动验证</p>
     */
    @Test
    void givenQoS0Publish_whenPublishToBrokerAndForward_then() {
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * QoS1 消息不进入 inQueue 队列; 进入 outQueue 队列
     * <p>观察 DEBUG 日志吧，没想到好办法自动验证</p>
     * <p>Broker(as Receiver) 接受 QoS1 Publish 不进入 Session.inQueue</p>
     * <p>Broker(as Sender) forward QoS1 Publish to receiver1 进入 Session.outQueue</p>
     */
    @Test
    void givenQoS1Publish_whenPublishToBrokerAndForward_then() {
        // receiver1 subscribe t/1 QoS1
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 1;
        Publish publish = Publish.outgoing(false, qos, false, "t/1",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        receiver1.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
    }


    /**
     * // given receiver1 subscribe t/0 (QoS 0) // when publish1 publish QoS0 Message to t/0 // then receiver1 receive a
     * QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given receiver1 subscribe t/0(QoS 0) // when publish1 publish QoS1 Message to t/0 // then receiver1 receive a
     * QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS1_thenReceiver1ReceiveQoS0() {
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given receiver1 subscribe t/0 QoS 0 // when publish1 publish QoS2 Message to t/0 // then receiver1 receive a
     * QoS0 Message from t/0
     */
    @Test
    void givenSubscribeQoS0_whenPublishQoS2_thenReceiver1ReceiveQoS0() {
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given receiver1 subscribe t/1 (QoS 1) // when publish1 publish QoS1 Message to t/1 // then receiver1 receive a
     * QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/1", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        receiver1.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
        // publish1 收到 PubAck 消息
        PubAck pubAck = new PubAck(publish1.readOutbound());
        then(pubAck.packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * // given receiver1 subscribe t/1 QoS 1 // when publish1 publish QoS0 Message to t/1 // then receiver1 receive a
     * QoS0 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/1", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given receiver1 subscribe t/1 QoS 1 // when publish1 publish QoS2 Message to t/1 // then receiver1 receive a
     * QoS1 Message from t/1
     */
    @Test
    void givenSubscribeQoS1_whenPublishQoS2_thenReceiver1ReceiveQoS1() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/1", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receiver1 receive QoS1 Message, should send a PubAck
        receiver1.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
        // publish1 receive PubRec, should send a PubRel
        then(new PubRec(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        publish1.writeInbound(PubRel.from(publish1PacketId).toByteBuf());
        // publish1 receive PubComp
        then(new PubComp(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * // given receiver1 subscribe t/2 QoS 2 // when publish1 publish QoS2 Message to t/2 // then receiver1 receive a
     * QoS2 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS2_thenReceiver1ReceiveQoS2() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(2, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubRec 消息
        receiver1.writeInbound(PubRec.from(packet.packetIdentifier()).toByteBuf());
        // receiver1 收到 PubRel
        then(new PubRel(receiver1.readOutbound()).packetIdentifier()).isEqualTo(packet.packetIdentifier());
        // Receiver1 回复 PubComp
        receiver1.writeInbound(PubComp.from(packet.packetIdentifier()).toByteBuf());
        //
        // publish1 收到 PubRec 消息
        then(new PubRec(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
        publish1.writeInbound(PubRel.from(publish1PacketId).toByteBuf());
        then(new PubComp(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * // given receiver1 subscribe t/2 QoS 2 // when publish1 publish QoS0 Message to t/2 // then receiver1 receive a
     * QoS0 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS0_thenReceiver1ReceiveQoS0() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(0, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given receiver1 subscribe t/2 QoS 2 // when publish1 publish QoS1 Message to t/2 // then receiver1 receive a
     * QoS1 Message from t/2
     */
    @Test
    void givenSubscribeQoS2_whenPublishQoS1_thenReceiver1ReceiveQoS1() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        SubAck subAck = new SubAck(receiver1.readOutbound());
        then(subAck.packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        short publish1PacketId = sPublish1.nextPacketIdentifier();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/2", publish1PacketId,
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // receive1 收到 Publish 消息后回复 PubAck 消息
        receiver1.writeInbound(PubAck.from(packet.packetIdentifier()).toByteBuf());
        //
        // publish1 收到 PubAck
        then(new PubAck(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish1PacketId);
    }

    /**
     * <p>订阅 -> 可以接收到消息</p>
     * <p>取消订阅 -> 无法接收到消息</p>
     */
    @Test
    void givenSubscription_whenPublish_thenWillReceivePublish() {
        // given
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        List<Subscribe.Subscription> subscriptions = singletonList(new Subscribe.Subscription("t/0", 0));
        receiver1.writeInbound(Subscribe.from(pId, subscriptions).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId);
        // when
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        // then
        Publish m = new Publish(receiver1.readOutbound());
        then(m).isNotNull()
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // 取消订阅
        // given
        receiver1.writeInbound(Unsubscribe.from(sReceiver1.nextPacketIdentifier(), subscriptions).toByteBuf());
        then(new UnsubAck(receiver1.readOutbound())).isNotNull();
        // when
        Publish p2 = Publish.outgoing(false, qos, false, "t/0",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(p2.toByteBuf());
        // then
        then(receiver1.<ByteBuf>readOutbound()).isNull();
    }

    /**
     * // given Client has no Subscription // when forward Publish // then Client receive no Publish
     */
    @Test
    void givenNoSubscription_whenPublish_thenWillNotReceivePublish() {
        // given
        // when
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        // then
        ByteBuf receivedPacket = receiver1.readOutbound();
        then(receivedPacket).isNull();
    }

    /**
     * // given receiver1 subscribe t/0 QoS 0 receiver1 subscribe t/0/# QoS 0 // when publish1 publish QoS0 Message to
     * t/0 // then receiver1 receive a QoS0 message from t/0 receiver1 receive a QoS0 message from t/0/#
     */
    @Test
    void givenSubscribe2TopicQoS0_whenPublish_thenReceive2Message() {
        // receiver1 subscribe t/0 QoS0
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/0", 0))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId);
        // receiver1 subscribe t/0/# QoS0
        short pId2 = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId2, singletonList(new Subscribe.Subscription("t/0/#", 0))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId2);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        byte qos = (byte) 0;
        Publish publish = Publish.outgoing(false, qos, false, "t/0", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage1 = new Publish(receiver1.readOutbound());
        then(publishMessage1)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage2 = new Publish(receiver1.readOutbound());
        then(publishMessage2)
            .returns((int) qos, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        then(publishMessage1.packetIdentifier()).isEqualTo((byte) 0);
        then(publishMessage2.packetIdentifier()).isEqualTo((byte) 0);
    }

    /**
     * // given receiver1 subscribe t/1 QoS 1 receiver1 subscribe t/1/# QoS 1 // when publish1 publish QoS1 Message to
     * t/1 // then receiver1 receive a QoS1 message from t/1 receiver1 receive a QoS1 message from t/1/#
     */
    @Test
    void givenSubscribe2TopicQoS1_whenPublish_thenReceive2Message() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/1", 1))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId);
        short pId2 = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId2, singletonList(new Subscribe.Subscription("t/1/#", 1))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId2);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/1", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage1 = (Publish) ControlPacket.from(receiver1.readOutbound());
        then(publishMessage1)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        receiver1.writeInbound(PubAck.from(publishMessage1.packetIdentifier()).toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage2 = new Publish(receiver1.readOutbound());
        then(publishMessage2)
            .returns(1, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        then(publishMessage1.packetIdentifier()).isNotEqualTo(publishMessage2.packetIdentifier());
        //
        receiver1.writeInbound(PubAck.from(publishMessage2.packetIdentifier()).toByteBuf());
        then(new PubAck(publish1.readOutbound())).isNotNull();
    }

    /**
     * // given receiver1 subscribe t/2 QoS 2 receiver1 subscribe t/2/# QoS 2 // when publish1 publish QoS2 Message to
     * t/2 // then receiver1 receive a QoS2 message from t/2 receiver1 receive a QoS2 message from t/2/#
     */
    @Test
    void givenSubscribe2TopicQoS2_whenPublish_thenReceive2Message() {
        short pId = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId, singletonList(new Subscribe.Subscription("t/2", 2))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId);
        short pId2 = sReceiver1.nextPacketIdentifier();
        receiver1.writeInbound(Subscribe.from(pId2, singletonList(new Subscribe.Subscription("t/2/#", 2))).toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId2);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/2", sPublish1.nextPacketIdentifier(),
            Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // publish1 收到 PubRec 消息
        then(new PubRec(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish.packetIdentifier());
        publish1.writeInbound(PubRel.from(publish.packetIdentifier()).toByteBuf());
        then(new PubComp(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish.packetIdentifier());
        //
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage1 = new Publish(receiver1.readOutbound());
        then(publishMessage1)
            .returns(2, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        receiver1.writeInbound(PubRec.from(publishMessage1.packetIdentifier()).toByteBuf());
        then(new PubRel(receiver1.readOutbound()).packetIdentifier()).isEqualTo(publishMessage1.packetIdentifier());
        receiver1.writeInbound(PubComp.from(publishMessage1.packetIdentifier()).toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage2 = new Publish(receiver1.readOutbound());
        then(publishMessage2)
            .returns(2, Publish::qos)
            .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        then(publishMessage1.packetIdentifier()).isNotEqualTo(publishMessage2.packetIdentifier());
        // receiver1
        receiver1.writeInbound(PubRec.from(publishMessage2.packetIdentifier()).toByteBuf());
        then(new PubRel(receiver1.readOutbound()).packetIdentifier()).isEqualTo(publishMessage2.packetIdentifier());
        receiver1.writeInbound(PubComp.from(publishMessage2.packetIdentifier()).toByteBuf());
    }

    /**
     * retain 消息测试
     * <p>正常 forward 消息的话 retain==0</p>
     * <p>后续订阅匹配 retain 消息的话 retain==1</p>
     */
    @Test
    void givenRetain_whenRetainPublish_then() {
        short pId = sReceiver1.nextPacketIdentifier();
        Subscribe sub1 = Subscribe.from(pId, singletonList(new Subscribe.Subscription("retain/#", 0)));
        receiver1.writeInbound(sub1.toByteBuf());
        // 读出 SubAck 消息
        then(new SubAck(receiver1.readOutbound()).packetIdentifier()).isEqualTo(pId);
        // publish1 发送 Publish 消息
        String strPayload = UUID.randomUUID().toString();
        Publish retain = Publish.outgoing(true, (byte) 0, false, "retain/1",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(retain.toByteBuf());
        // receiver1 接受 Publish
        // 正常 forward 路由的 Publish
        then(new Publish(receiver1.readOutbound()).retainFlag()).isFalse();
        //
        // 后续订阅
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(),
            singletonList(new Subscribe.Subscription("retain/1", 0)));
        receiver1.writeInbound(sub2.toByteBuf());
        then(new SubAck(receiver1.readOutbound())).isNotNull();
        // 收到 retain 消息
        then(new Publish(receiver1.readOutbound()).retainFlag()).isTrue();
    }

    /**
     * retain 消息测试
     * <p>发送第1条 retain Publish 到 retain/1</p>
     * <p>发送第2条 retain Publish 到 retain/1</p>
     * <p>Broker 仅存储最后一条 retain Publish</p>
     */
    @Test
    void givenRetain_whenRetain2Times_thenOnlyStoreTheLast() {
        // publish1 发送 Publish 消息
        Publish retain1 = Publish.outgoing(true, (byte) 0, false, "retain/1",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(UUID.randomUUID().toString(), UTF_8));
        publish1.writeInbound(retain1.toByteBuf());
        // 发送第2条 retain Publish
        String retain2Payload = UUID.randomUUID().toString();
        Publish retain = Publish.outgoing(true, (byte) 0, false, "retain/1",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(retain2Payload, UTF_8));
        publish1.writeInbound(retain.toByteBuf());
        //
        // 后续订阅
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(),
            singletonList(new Subscribe.Subscription("retain/1", 0)));
        receiver1.writeInbound(sub2.toByteBuf());
        then(new SubAck(receiver1.readOutbound())).isNotNull();
        // 收到 retain 消息
        then(new Publish(receiver1.readOutbound()))
            .returns(true, Publish::retainFlag)
            .returns(retain2Payload, p -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * retain 消息取消测试
     * <p>发送第1条 retain Publish 到 retain/1</p>
     * <p>发送第2条 retain Publish (payload has zero bytes)到 retain/1</p>
     * <p>Broker 丢弃之前存储的 retain 消息</p>
     */
    @Test
    void givenRetain_whenSendZeroByteRetainPublish_thenNoRetain() {
        // publish1 发送 Publish 消息
        Publish retain1 = Publish.outgoing(true, (byte) 1, false, "retain/1",
            sPublish1.nextPacketIdentifier(), Unpooled.copiedBuffer(UUID.randomUUID().toString(), UTF_8));
        publish1.writeInbound(retain1.toByteBuf());
        // 发送第2条 retain Publish (no payload)
        Publish retain = Publish.outgoing(true, (byte) 1, false, "retain/1",
            sPublish1.nextPacketIdentifier(), Unpooled.buffer());
        publish1.writeInbound(retain.toByteBuf());
        //
        // 后续订阅
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(),
            singletonList(new Subscribe.Subscription("retain/1", 0)));
        receiver1.writeInbound(sub2.toByteBuf());
        then(new SubAck(receiver1.readOutbound())).isNotNull();
        // 不会收到 retain 消息
        ByteBuf buf = receiver1.readOutbound();
        then(buf).isNull();
    }

    /**
     * Will Message 测试
     * <p>Connect has Will message</p>
     * <p>发送 Disconnect 后断开连接，不触发 Will Message</p>
     */
    @Test
    void givenWillConnect_whenDisconnect_thenNoWillMessage() {
        // given
        receiver1.writeInbound(Subscribe.from(sReceiver1.nextPacketIdentifier(),
            singletonList(new Subscribe.Subscription("will/1", 2))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        //
        publish1 = createChannel(broker);
        String willContent = "I'm a Will Message.";
        Connect willConnect = Connect.from(2, false,
            (short) 64,
            "publish1",
            "will/1", Unpooled.copiedBuffer(willContent, UTF_8));
        // publish1 模拟接受 Connect 消息
        publish1.writeInbound(willConnect.toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(publish1.readOutbound())).isNotNull();
        // when
        // Disconnect from Peer
        publish1.writeInbound(Disconnect.from().toByteBuf());
        // then
        then(receiver1.<ByteBuf>readOutbound()).isNull();
    }

    /**
     * Will Message 测试
     * <p>Connect has Will message</p>
     * <p>未发送 Disconnect 连接断开，触发 Will Message</p>
     */
    @Test
    void givenWillConnect_whenLostConnection_thenOtherClientReceiveWillMessage() {
        // given
        receiver1.writeInbound(Subscribe.from(sReceiver1.nextPacketIdentifier(),
            singletonList(new Subscribe.Subscription("will/1", 2))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        //
        publish1 = createChannel(broker);
        String willContent = "I'm a Will Message.";
        Connect willConnect = Connect.from(2, false,
            (short) 64,
            "publish1",
            "will/1", Unpooled.copiedBuffer(willContent, UTF_8));
        // publish1 模拟接受 Connect 消息
        publish1.writeInbound(willConnect.toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(publish1.readOutbound())).isNotNull();
        //
        // when
        // close the Channel before receive Disconnect from Peer
        publish1.close();
        // then
        Publish willMessage = new Publish(receiver1.readOutbound());
        then(willMessage).returns(2, Publish::qos)
            .returns(willContent, p -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

}