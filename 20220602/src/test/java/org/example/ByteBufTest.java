package org.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
class ByteBufTest {

    /**
     * Creates a new big-endian Java heap buffer
     */
    @Test
    void givenByteBuf_whenCreateHeapByteBuf_then() {
        final ByteBuf buffer = Unpooled.buffer(4, 8);
        then(buffer.capacity()).isEqualTo(4);
        then(buffer.maxCapacity()).isEqualTo(8);
    }

    /**
     * Creates a new big-endian direct buffer
     */
    @Test
    void givenByteBuf_whenCreateDirectByteBuf_then() {
        final ByteBuf buffer = Unpooled.directBuffer(4, 8);
        then(buffer.capacity()).isEqualTo(4);
        then(buffer.maxCapacity()).isEqualTo(8);
    }

    /**
     * ByteBuf 指定大小上限后，超出后抛 IndexOutOfBoundsException
     */
    @Test
    void givenByteBuf_whenGrowUpToMaxCapacity_then() {
        final ByteBuf buffer = Unpooled.buffer(4, 8);
        then(buffer.maxWritableBytes()).isEqualTo(8);
        buffer.writeLong(1L);
        final Throwable throwable = catchThrowable(() -> buffer.writeInt(1));
        then(throwable).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /**
     * <pre>
     * Random Access
     * get*() 可以 random access
     * read*() 不可以
     * write*() 不可以
     * </pre>
     */
    @Test
    void givenByteBuf_whenRandomGetAccess_then() {
        final ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(1L);
        final int val = 2;
        buffer.writeInt(val);
        then(buffer.getInt(8)).isEqualTo(val);
    }

    @Test
    void givenByteBuf_whenSearch_then() {
        final ByteBuf buffer = Unpooled.buffer();
        // write 8 bytes
        byte[] str = "Hello, World".getBytes(StandardCharsets.UTF_8);
        if (buffer.maxWritableBytes() >= str.length) {
            buffer.writeBytes(str);
        }
        if (buffer.maxWritableBytes() > 0) {
            buffer.writeByte(0x00);
        }
        then(buffer.bytesBefore((byte) 0x00)).isEqualTo(str.length);
    }

    /**
     * <pre>
     * duplicate() has same readerIndex and writerIndex of the origin buff
     * slice() readerIndex == 0
     * </pre>
     */
    @Test
    void givenByteBuf_whenDerive_then() {
        final ByteBuf buffer = Unpooled.buffer();
        byte[] str = "Hello, World".getBytes(StandardCharsets.UTF_8);
        if (buffer.maxWritableBytes() >= str.length) {
            buffer.writeBytes(str);
        }
        then(buffer.readByte()).isEqualTo((byte) 'H');
        // duplicate
        final ByteBuf duplicate = buffer.duplicate();
        then(duplicate.readerIndex()).isEqualTo(buffer.readerIndex());
        then(duplicate.writerIndex()).isEqualTo(buffer.writerIndex());
        // slice
        final ByteBuf slice = buffer.slice();
        then(slice.readableBytes()).isEqualTo(buffer.readableBytes());
        then(slice.readerIndex()).isEqualTo(0);
    }

    @Test
    void givenByteBuf_whenReadBytesToByteBuf_then() {
        ByteBuf buffer = Unpooled.buffer();
        byte[] str = "Hello, World".getBytes(StandardCharsets.UTF_8);
        if (buffer.maxWritableBytes() >= str.length) {
            buffer.writeBytes(str);
        }
        ByteBuf destination = Unpooled.buffer(2);
        then(destination.writableBytes()).isEqualTo(2);
        buffer.readBytes(destination);
        then(destination.writableBytes()).isEqualTo(0);
        then(destination.readableBytes()).isEqualTo(2);
    }

    /**
     * 测试 Unpooled.heapBuffer() 的内存释放
     */
    @Test
    void givenUnpooledHeapBuffer_whenRelease_then() {
        ByteBuf heapBuf = Unpooled.buffer();
        then(heapBuf.refCnt()).isEqualTo(1);
        ByteBuf retain = heapBuf.retain();
        then(retain).isSameAs(heapBuf);
        then(heapBuf.refCnt()).isEqualTo(2);
        then(heapBuf.release()).isFalse();
        then(heapBuf.release()).isTrue();
    }

}
