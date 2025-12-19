package org.github.zzf.mqtt.protocol.model;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.junit.jupiter.api.Test;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/07/01
 */
@Slf4j
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
        then(in.retainFlag()).isFalse();
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
     * toString 测试
     */
    @Test
    void given_whenToString_then() {
        // given
        Publish publish = Publish.outgoing(false, (byte) 0, false, "", (short) 0, Unpooled.buffer());
        then(publish.toString()).isNotNull();
    }

    @Test
    void given_whenGetClassName_then() {
        String name = AbstractSession.class.getName();
        String simpleName = AbstractSession.class.getSimpleName();
        String canonicalName = AbstractSession.class.getCanonicalName();
        String typeName = AbstractSession.class.getTypeName();
        log.info("name: {}\nsimpleName: {}\ncanonicalName: {}\ntypeName: {}",
                name, simpleName, canonicalName, typeName);
    }


    /**
     * 测试 toByteBuf
     */
    @Test void given_whenToByteBuf_then() {
        ByteBuf buf = Unpooled.copiedBuffer(new byte[256]);
        ByteBuf timestamp = Unpooled.buffer(16)
            .writeLong(System.nanoTime())
            .writeLong(System.currentTimeMillis());
        CompositeByteBuf payload = Unpooled.compositeBuffer()
            .addComponents(true, timestamp, buf);
        //
        ByteBuf byteBuf = Publish
            .outgoing(false, 0, false, "Topic", (short) 1, payload)
            .toByteBuf();
        byteBuf.release();
        then(byteBuf.refCnt()).isEqualTo(0);
        // 所有的底层 ByteBuf 都会被释放掉，因为 CompositeByteBuf 引用了所有的底层 ByteBuf
        then(payload.refCnt()).isEqualTo(0);
        then(timestamp.refCnt()).isEqualTo(0);
        then(buf.refCnt()).isEqualTo(0);
        then(catchThrowable(payload::release))
            .isNotNull()
            .isInstanceOf(IllegalReferenceCountException.class);
    }

}