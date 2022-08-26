package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/01
 */
class PublishTest {

    /**
     * 正常流程测试
     */
    @Test
    void givenRightPacket_whenOutAndIn_thenSuccess() {
        ByteBuf payload = Unpooled.copyLong(1L, 2L, 3L);
        String topicName = "topicName";
        Publish out = Publish.outgoing(false, (byte) Publish.AT_LEAST_ONCE, false, topicName, Short.MAX_VALUE, payload);
        then(out.packetValidate()).isTrue();
        ByteBuf buf = out.toByteBuf();
        // inBound
        Publish in = (Publish) Publish.from(buf);
        then(in.packetValidate()).isTrue();
        then(in.atLeastOnce()).isTrue();
        then(in.retain()).isFalse();
        then(in.dup()).isFalse();
        then(in.topicName()).isEqualTo(topicName);
        then(in.payload().readableBytes()).isEqualTo(3 * 8);
    }

    @Test
    void given_whenBuild_0Byte_then() {
        then(Publish.build_0Byte(true, (byte) 1, true)).isEqualTo((byte) 0x3B);
        then(Publish.build_0Byte(true, (byte) 2, true)).isEqualTo((byte) 0x3D);
        then(Publish.build_0Byte(false, (byte) 2, true)).isEqualTo((byte) 0x3C);
        then(Publish.build_0Byte(false, (byte) 2, false)).isEqualTo((byte) 0x34);
    }


    /**
     * dup 标识位设置
     */
    @Test
    void given_whenSetDup_then() {
        // given
        Publish publish = Publish.outgoing(false, (byte) 0, false, "", (short) 0, Unpooled.buffer());
        // then
        then(publish.dup()).isFalse();
        // then
        then(publish.dup(true).dup()).isTrue();
    }

    /**
     * toString 测试
     */
    @Test
    void given_whenToString_then() {
        // given
        Publish publish = Publish.outgoing(false, (byte) 0, false, "", (short) 0, Unpooled.buffer());
        publish.packetIdentifier(Short.MIN_VALUE);
        then(publish.toString()).isNotNull();
    }

}