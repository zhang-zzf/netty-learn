package org.example.mqtt.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import org.example.mqtt.broker.jvm.DefaultBroker;
import org.example.mqtt.bootstrap.MqttCodec;
import org.example.mqtt.model.*;
import org.example.mqtt.session.AbstractSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.BDDAssertions.then;

class ServerSessionHandlerTest {

    // receiver1
    Session0 sReceiver1;
    EmbeddedChannel receiver1;

    // publish1
    Session0 sPublish1;
    EmbeddedChannel publish1;

    @BeforeEach
    public void beforeEach() {
        // 每个 UT 一个新的 Broker
        final Broker broker = new DefaultBroker();
        final String strReceiver01 = "receiver1";
        sReceiver1 = new Session0(strReceiver01);
        receiver1 = new EmbeddedChannel();
        // receiver1 Connect to the Broker
        receiver1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
        // receiver 模拟接受 Connect 消息
        receiver1.writeInbound(Connect.from(strReceiver01, (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(receiver1.readOutbound())).isNotNull();
        //
        sPublish1 = new Session0("publish1");
        publish1 = new EmbeddedChannel();
        publish1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
        // publish1 模拟接受 Connect 消息
        publish1.writeInbound(Connect.from("publish1", (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(publish1.readOutbound())).isNotNull();
    }

    /**
     * // given
     * receiver1 subscribe t/0 QoS 0
     * // when
     * publish1 publish Message to t/0 QoS0
     * // then
     * receiver1 receive a QoS0 Message from t/0
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
     * // given
     * receiver1 subscribe t/0 QoS 0
     * // when
     * publish1 publish Message to t/0 QoS1
     * // then
     * receiver1 receive a QoS0 Message from t/0
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
     * // given
     * receiver1 subscribe t/0 QoS 0
     * // when
     * publish1 publish Message to t/0 QoS1
     * // then
     * receiver1 receive a QoS0 Message from t/0
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
     * // given
     * receiver1 subscribe t/1 QoS 1
     * // when
     * publish1 publish QoS1 Message to t/1
     * // then
     * receiver1 receive a QoS1 Message from t/1
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
        Publish publish = Publish.outgoing(false, (byte) 1, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
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
     * // given
     * receiver1 subscribe t/1 QoS 1
     * // when
     * publish1 publish QoS0 Message to t/1
     * // then
     * receiver1 receive a QoS0 Message from t/1
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
        Publish publish = Publish.outgoing(false, (byte) 0, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
        publish1.writeInbound(publish.toByteBuf());
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish packet = new Publish(receiver1.readOutbound());
        then(packet)
                .returns(0, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
    }

    /**
     * // given
     * receiver1 subscribe t/1 QoS 1
     * // when
     * publish1 publish QoS2 Message to t/1
     * // then
     * receiver1 receive a QoS1 Message from t/1
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
        Publish publish = Publish.outgoing(false, (byte) 2, false, "t/1", publish1PacketId, Unpooled.copiedBuffer(strPayload, UTF_8));
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
     * // given
     * receiver1 subscribe t/2 QoS 2
     * // when
     * publish1 publish QoS2 Message to t/2
     * // then
     * receiver1 receive a QoS2 Message from t/2
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
     * // given
     * receiver1 subscribe t/2 QoS 2
     * // when
     * publish1 publish QoS0 Message to t/2
     * // then
     * receiver1 receive a QoS0 Message from t/2
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
     * // given
     * receiver1 subscribe t/2 QoS 2
     * // when
     * publish1 publish QoS1 Message to t/2
     * // then
     * receiver1 receive a QoS1 Message from t/2
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
     * // given
     * receiver1 subscribe t/0 QoS 0
     * receiver1 subscribe t/0/# QoS 0
     * // when
     * publish1 publish Message to t/0 QoS0
     * // then
     * receiver1 receive a QoS0 message from t/0
     * receiver1 receive a QoS0 message from t/0/#
     */
    @Test
    void givenSubscribe2TopicQoS0_whenPublish_thenReceiver1Receive2Message() {
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
        if (qos == 0) {
            then(publishMessage1.packetIdentifier()).isEqualTo((byte) 0);
            then(publishMessage2.packetIdentifier()).isEqualTo((byte) 0);
        } else {
            then(publishMessage1.packetIdentifier()).isNotEqualTo(publishMessage2.packetIdentifier());
        }
    }

    /**
     * // given
     * receiver1 subscribe t/1 QoS 1
     * receiver1 subscribe t/1/# QoS 1
     * // when
     * publish1 publish QoS1 Message to t/1
     * // then
     * receiver1 receive a QoS1 message from t/1
     * receiver1 receive a QoS1 message from t/1/#
     */
    @Test
    void givenSubscribe2TopicQoS1_whenPublish_thenReceiver1Receive2Message() {
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
        Publish publishMessage1 = new Publish(receiver1.readOutbound());
        then(publishMessage1)
                .returns(1, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage2 = new Publish(receiver1.readOutbound());
        then(publishMessage2)
                .returns(1, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        then(publishMessage1.packetIdentifier()).isNotEqualTo(publishMessage2.packetIdentifier());
        //
        receiver1.writeInbound(PubAck.from(publishMessage1.packetIdentifier()).toByteBuf());
        receiver1.writeInbound(PubAck.from(publishMessage2.packetIdentifier()).toByteBuf());
        then(new PubAck(publish1.readOutbound())).isNotNull();
    }

    /**
     * // given
     * receiver1 subscribe t/2 QoS 2
     * receiver1 subscribe t/2/# QoS 2
     * // when
     * publish1 publish QoS2 Message to t/2
     * // then
     * receiver1 receive a QoS2 message from t/2
     * receiver1 receive a QoS2 message from t/2/#
     */
    @Test
    void givenSubscribe2TopicQoS2_whenPublish_thenReceiver1Receive2Message() {
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
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage1 = new Publish(receiver1.readOutbound());
        then(publishMessage1)
                .returns(2, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        // Broker forward 后 receiver1 接受 Publish 消息
        Publish publishMessage2 = new Publish(receiver1.readOutbound());
        then(publishMessage2)
                .returns(2, Publish::qos)
                .returns(strPayload, (p) -> p.payload().readCharSequence(p.payload().readableBytes(), UTF_8));
        then(publishMessage1.packetIdentifier()).isNotEqualTo(publishMessage2.packetIdentifier());
        // publish1 收到 PubRec 消息
        then(new PubRec(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish.packetIdentifier());
        publish1.writeInbound(PubRel.from(publish.packetIdentifier()).toByteBuf());
        then(new PubComp(publish1.readOutbound()).packetIdentifier()).isEqualTo(publish.packetIdentifier());
        // receiver1
        receiver1.writeInbound(PubRec.from(publishMessage1.packetIdentifier()).toByteBuf());
        receiver1.writeInbound(PubRec.from(publishMessage2.packetIdentifier()).toByteBuf());
        then(new PubRel(receiver1.readOutbound()).packetIdentifier()).isEqualTo(publishMessage1.packetIdentifier());
        then(new PubRel(receiver1.readOutbound()).packetIdentifier()).isEqualTo(publishMessage2.packetIdentifier());
        receiver1.writeInbound(PubComp.from(publishMessage1.packetIdentifier()).toByteBuf());
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
        then(new Publish(receiver1.readOutbound()).retain()).isFalse();
        //
        // 后续订阅
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(), singletonList(new Subscribe.Subscription("retain/1", 0)));
        receiver1.writeInbound(sub2.toByteBuf());
        then(new SubAck(receiver1.readOutbound())).isNotNull();
        // 收到 retain 消息
        then(new Publish(receiver1.readOutbound()).retain()).isTrue();
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
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(), singletonList(new Subscribe.Subscription("retain/1", 0)));
        receiver1.writeInbound(sub2.toByteBuf());
        then(new SubAck(receiver1.readOutbound())).isNotNull();
        // 收到 retain 消息
        then(new Publish(receiver1.readOutbound()))
                .returns(true, Publish::retain)
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
        Subscribe sub2 = Subscribe.from(sReceiver1.nextPacketIdentifier(), singletonList(new Subscribe.Subscription("retain/1", 0)));
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
        final Broker broker = new DefaultBroker();
        final String strReceiver01 = "receiver1";
        sReceiver1 = new Session0(strReceiver01);
        receiver1 = new EmbeddedChannel();
        // receiver1 Connect to the Broker
        receiver1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
        // receiver 模拟接受 Connect 消息
        receiver1.writeInbound(Connect.from("receiver1", (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(receiver1.readOutbound())).isNotNull();
        receiver1.writeInbound(Subscribe.from(sReceiver1.nextPacketIdentifier(),
                singletonList(new Subscribe.Subscription("will/1", 2))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        //
        sPublish1 = new Session0("publish1");
        publish1 = new EmbeddedChannel();
        publish1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
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
        final Broker broker = new DefaultBroker();
        final String strReceiver01 = "receiver1";
        sReceiver1 = new Session0(strReceiver01);
        receiver1 = new EmbeddedChannel();
        // receiver1 Connect to the Broker
        receiver1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
        // receiver 模拟接受 Connect 消息
        receiver1.writeInbound(Connect.from("receiver1", (short) 64).toByteBuf());
        // 读出 ConnAck 消息
        then(new ConnAck(receiver1.readOutbound())).isNotNull();
        receiver1.writeInbound(Subscribe.from(sReceiver1.nextPacketIdentifier(),
                singletonList(new Subscribe.Subscription("will/1", 2))).toByteBuf());
        // 读出 SubAck 消息
        new SubAck(receiver1.readOutbound());
        //
        sPublish1 = new Session0("publish1");
        publish1 = new EmbeddedChannel();
        publish1.pipeline().addLast(new MqttCodec())
                .addLast(ServerSessionHandler.HANDLER_NAME, new ServerSessionHandler(broker, packet -> 0x00, 3));
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

    public static class Session0 extends AbstractSession {

        protected Session0(String clientIdentifier) {
            super(clientIdentifier);
        }

        @Override
        protected boolean onPublish(Publish packet, Future<Void> promise) {
            return false;
        }

        @Override
        public Set<Subscribe.Subscription> subscriptions() {
            return null;
        }

    }


}