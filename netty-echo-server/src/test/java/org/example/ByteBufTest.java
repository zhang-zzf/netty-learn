package org.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.IllegalReferenceCountException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
@Slf4j
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
        then(buffer.release()).isTrue();
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
        byte[] str = "Hello, World".getBytes(UTF_8);
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
        byte[] str = "Hello, World".getBytes(UTF_8);
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
        byte[] str = "Hello, World".getBytes(UTF_8);
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

    /**
     * 测试 Unpooled.directBuffer() 的内存释放
     */
    @Test
    void givenUnpooledDirectBuffer_whenRelease_then() {
        ByteBuf directBuffer = Unpooled.directBuffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        ByteBuf retain = directBuffer.retain();
        then(retain).isSameAs(directBuffer);
        then(directBuffer.refCnt()).isEqualTo(2);
        then(directBuffer.release()).isFalse();
        then(directBuffer.release()).isTrue();
    }

    /**
     * 测试 PooledByteBufAllocator.directBuffer() 的内存释放
     */
    @Test
    void givenPooledByteBufAllocator_whenAllocateReleaseDirectByteBuf_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        ByteBuf retain = directBuffer.retain();
        then(retain).isSameAs(directBuffer);
        then(directBuffer.refCnt()).isEqualTo(2);
        then(directBuffer.release()).isFalse();
        then(directBuffer.release()).isTrue();
    }

    /**
     * 测试 PooledByteBufAllocator.heapBuffer() 的内存释放
     */
    @Test
    void givenPooledByteBufAllocator_whenAllocateReleaseHeapBuffer_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf heapBuf = PooledByteBufAllocator.DEFAULT.heapBuffer();
        then(heapBuf.refCnt()).isEqualTo(1);
        ByteBuf retain = heapBuf.retain();
        then(retain).isSameAs(heapBuf);
        then(heapBuf.refCnt()).isEqualTo(2);
        then(heapBuf.release()).isFalse();
        then(heapBuf.release()).isTrue();
    }

    /**
     * <p>slice()</p>
     * <p>验证结果：source buffer release 后，访问 slice() 的 buffer 抛出 IllegalReferenceCountException </p>
     */
    @Test
    void givenPooledByteBufAllocator_whenSlice_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        ByteBuf noRetainedSlice = directBuffer.readSlice(8);
        // release directBuffer
        then(directBuffer.release()).isTrue();
        Throwable throwable = catchThrowable(noRetainedSlice::readLong);
        then(throwable).isNotNull().isInstanceOf(IllegalReferenceCountException.class);
    }

    /**
     * 测试 PooledByteBuf#retainedSlice() 的内存释放
     * <p>retainedSlice()</p>
     * <p>ByteBuf#readRetainedSlice() 后，ByteBuf 的 refCnt == 2, 新生成的 slice 的 refCnt == 1</p>
     */
    @Test
    void givenPooledByteBuf_whenRetainedSlice_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        //
        // will increase the source buffer's refCnt
        ByteBuf retainedSlice = directBuffer.retainedSlice();
        then(directBuffer.refCnt()).isEqualTo(2);
        // slice buf.refCnt() == 1
        then(retainedSlice.refCnt()).isEqualTo(1);
    }

    /**
     * 测试 UnpooledByteBuf#retainedSlice() 的内存释放
     * <p>retainedSlice()</p>
     * <p>ByteBuf#readRetainedSlice() 后，生成 UnpooledSlicedByteBuf 和源 ByteBuf 共享 refCnt </p>
     */
    @Test
    void givenUnpooledByteBuf_whenRetainedSlice_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = Unpooled.buffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        //
        // will increase the source buffer's refCnt
        ByteBuf retainedSlice = directBuffer.retainedSlice();
        then(directBuffer.refCnt()).isEqualTo(2);
        then(retainedSlice.refCnt()).isEqualTo(2);
    }

    /**
     * 测试 PooledByteBuf#slice() / UnpooledByteBuf#slice() 的内存释放
     * <p>slice()</p>
     * <p>ByteBuf#slice() 后，生成 UnpooledSlicedByteBuf 和源 ByteBuf 共享 refCnt </p>
     */
    @Test
    void givenByteBuf_whenSliceAndRetain_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf pooledBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        then(pooledBuf.refCnt()).isEqualTo(1);
        pooledBuf.writeLong(Integer.MAX_VALUE);
        // will increase the source buffer's refCnt
        ByteBuf slice = pooledBuf.slice();
        slice.retain();
        then(pooledBuf.refCnt()).isEqualTo(2);
        then(slice.refCnt()).isEqualTo(2);
        // 使用 Unpooled ByteBuf
        ByteBuf unpooled = Unpooled.buffer();
        ByteBuf unpooledSliceByteBuf = unpooled.slice();
        unpooledSliceByteBuf.retain();
        then(unpooled.refCnt()).isEqualTo(2);
        then(unpooledSliceByteBuf.refCnt()).isEqualTo(2);
    }

    /**
     * 测试 PooledByteBuf#readRetainedSlice() 的内存释放
     * <p>readRetainedSlice() 会更改 source 的 readerIndex </p>
     * <p>ByteBuf#readRetainedSlice() 后，ByteBuf 的 refCnt == 2, 新生成的 slice 的 refCnt == 1</p>
     */
    @Test
    void givenPooledByteBuf_whenReadRetainedSlice_then2() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        //
        // will increase the source bufffer's refCnt
        ByteBuf retainedSlice = directBuffer.readRetainedSlice(8);
        then(directBuffer.refCnt()).isEqualTo(2);
        // slice buf.refCnt() == 1
        then(retainedSlice.refCnt()).isEqualTo(1);
    }

    /**
     * 测试 UnpooledByteBuf#readRetainedSlice() 的内存释放
     * <p>readRetainedSlice() 会更改 source 的 readerIndex </p>
     * <p>UnpooledByteBuf#readRetainedSlice() 后，ByteBuf 的 refCnt == 2,
     * 新生成的 UnpooledSliceByteBuf ref==2 和源 ByteBuf 共享 refCnt </p>
     */
    @Test
    void givenUnpooledByteBuf_whenReadRetainedSlice_then() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = Unpooled.buffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        //
        // will increase the source bufffer's refCnt
        ByteBuf retainedSlice = directBuffer.readRetainedSlice(8);
        then(directBuffer.refCnt()).isEqualTo(2);
        // slice buf.refCnt() == 2
        then(retainedSlice.refCnt()).isEqualTo(2);
    }

    /**
     * 测试 PooledByteBuf#readRetainedSlice() 的内存释放
     * <p>readRetainedSlice()</p>
     * <p>PooledByteBuf#readRetainedSlice() 后，ByteBuf 的 refCnt == 2, 新生成的 slice 的 refCnt == 1</p>
     * <p>source ByteBuf 和 slice ByteBuf 分别管理自己的 refCnt。</p>
     * <p>核心点：slice ByteBuf released 后会引发 source ByteBuf 的 refCnt 减1</p>
     */
    @Test
    void givenPooledByteBufAllocator_whenRetainedSlice_then2() {
        // 使用 PoolByteBufAllocator
        ByteBuf directBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();
        then(directBuffer.refCnt()).isEqualTo(1);
        directBuffer.writeLong(Integer.MAX_VALUE);
        //
        // will increase the source buffer's refCnt
        ByteBuf retainedSlice = directBuffer.readRetainedSlice(8);
        then(directBuffer.refCnt()).isEqualTo(2);
        // slice buf.refCnt() == 1
        then(retainedSlice.refCnt()).isEqualTo(1);
        //
        // increase slice buf's refCnt
        retainedSlice.retain();
        then(retainedSlice.refCnt()).isEqualTo(2);
        //  source buf's refCnt do not change
        then(directBuffer.refCnt()).isEqualTo(2);
        //
        // increase source buf's refCnt
        directBuffer.retain();
        then(directBuffer.refCnt()).isEqualTo(3);
        then(retainedSlice.refCnt()).isEqualTo(2);
        //
        // release slice
        then(retainedSlice.release()).isFalse();
        then(retainedSlice.release()).isTrue();
        // when slice buf was released (refCnt == 0), do release the source
        then(directBuffer.refCnt()).isEqualTo(2);
        then(directBuffer.release()).isFalse();
        then(directBuffer.release()).isTrue();
    }

    /**
     * CompositeByteBuf#release() Composite 被释放后，不管底层有没有被释放， Composite 都不可以再被访问
     */
    @Test
    void givenCompositeByteBuf_whenRetainedAndReleaseComposite2_then() {
        ByteBuf header = Unpooled.buffer(8);
        header.writeInt(Integer.MAX_VALUE);
        header.retain();
        ByteBuf body = Unpooled.buffer(32);
        body.writeLong(Long.MAX_VALUE);
        CompositeByteBuf req = Unpooled.compositeBuffer()
            .addComponent(true, header)
            .addComponent(true, body);
        then(req.refCnt()).isEqualTo(1);
        // release CompositeByteBuf 会 release 底层的每个 ByteBuf
        then(req.release()).isTrue();
        then(req.refCnt()).isEqualTo(0);
        then(header.refCnt()).isEqualTo(1);
        then(catchThrowable(req::readInt)).isNotNull();
        //
        then(catchThrowable(body::readLong)).isNotNull();
        then(catchThrowable(header::readInt)).isNull();
    }

    /**
     * CompositeByteBuf#release() 实际上是 release 底层的每个 Component
     */
    @Test
    void givenCompositeByteBuf_whenRetainedAndReleaseComposite_then() {
        ByteBuf header = Unpooled.buffer(8);
        header.writeInt(Integer.MAX_VALUE);
        ByteBuf body = Unpooled.buffer(32);
        body.writeLong(Long.MAX_VALUE);
        CompositeByteBuf req = Unpooled.compositeBuffer()
            .addComponent(true, header)
            .addComponent(true, body);
        then(req.refCnt()).isEqualTo(1);
        // release CompositeByteBuf 会 release 底层的每个 ByteBuf
        then(req.release()).isTrue();
        then(header.refCnt()).isEqualTo(0);
        then(body.refCnt()).isEqualTo(0);
        //
        // 访问底层 Component 会抛出异常
        then(catchThrowable(header::readInt)).isNotNull();
    }

    /**
     * Composite.retain 不会操作底层的的 Component 的 refCnt
     */
    @Test
    void givenCompositeByteBuf_whenRetainedCompositeAndReleaseComponent_then() {
        ByteBuf header = Unpooled.buffer(8);
        header.writeInt(Integer.MAX_VALUE);
        ByteBuf body = Unpooled.buffer(32);
        body.writeLong(Long.MAX_VALUE);
        CompositeByteBuf req = Unpooled.compositeBuffer()
            .addComponent(true, header)
            .addComponent(true, body);
        then(req.refCnt()).isEqualTo(1);
        // retain the CompositeByteBuf
        req.retain();
        // Composite.retain 不会操作底层的的 Component 的 refCnt
        then(req.refCnt()).isEqualTo(2);
        then(header.refCnt()).isEqualTo(1);
        then(body.refCnt()).isEqualTo(1);
        // 把 Composite 的 ref-=1
        // 若 Composite.ref>0，不操作底层的 Component.refCnt
        then(req.release()).isFalse();
        then(req.refCnt()).isEqualTo(1);
        then(header.refCnt()).isEqualTo(1);
        then(body.refCnt()).isEqualTo(1);
        // 若 Composite.ref=0，release all its Component
        then(req.release()).isTrue();
        then(req.refCnt()).isEqualTo(0);
        then(header.refCnt()).isEqualTo(0);
        then(body.refCnt()).isEqualTo(0);
    }

    /**
     * CompositeByteBuf 底层的 Component 被释放，导致 CompositeByteBuf 访问抛出异常。
     */
    @Test
    void givenCompositeByteBuf_whenRetainedAndRelease_then() {
        ByteBuf header = Unpooled.buffer(8);
        header.writeInt(Integer.MAX_VALUE);
        ByteBuf body = Unpooled.buffer(32);
        body.writeLong(Long.MAX_VALUE);
        CompositeByteBuf req = Unpooled.compositeBuffer()
            .addComponent(true, header)
            .addComponent(true, body);
        // 释放底层 Component
        then(header.release()).isTrue();
        then(req.refCnt()).isEqualTo(1);
        Throwable throwable = catchThrowable(req::readInt);
        then(throwable).isNotNull();
    }

    /**
     * unicode 可以表示所有的汉字， but
     * <pre>
     *         基本汉字	        20902字	    4E00-9FA5
     *         基本汉字补充	    90字	    9FA6-9FFF
     *         扩展A	            6592字	    3400-4DBF
     *         汉字结构	        16字	    2FF0-2FFF
     *         汉语注音	        43字	    3105-312F
     *         康熙部首	        214字	    2F00-2FD5
     *         部首扩展	        115字①	    2E80-2EF3
     *         兼容汉字	        472字②	    F900-FAD9
     *         // 以上汉字,只占一个字符，也就是一个char，也就是2字节，也就是16位
     *
     *
     *          * 扩展B	42720字	20000-2A6DF
     *          * 扩展C	4154字	2A700-2B739
     *          * 扩展D	222字	2B740-2B81D
     *          * 扩展E	5762字	2B820-2CEA1
     *          * 扩展F	7473字	2CEB0-2EBE0
     *          * 扩展G	4939字	30000-3134A
     *          * 扩展H	4192字	31350-323AF
     *          * 扩展I	622字	2EBF0-2EE5D
     *          * 以上汉字需使用 2 个 char 表示，也即是 4 个字节
     *          * 这些汉字无法赋值给 char 类型的变量
     *
     * </pre>
     */
    @Test
    void given中文_when_then() {
        //Unicode编码:4E00
        then("一".length()).isEqualTo(1);
        then("一".toCharArray()).contains('一');
        //Unicode编码:9FA5
        then("龥".length()).isEqualTo(1);
        then("龥".toCharArray()).contains('龥');
        // Unicode编码:2B739 `𫜹` 占两个 unicode 字符，也就是两个 char，也就是4字节，也就是32位
        // char c = '𫜹';//当我们设置一个占多字符的汉字给char的时候，编译器会报错
        // Too many characters in character literal
        String oneWordUseTwoChar = "\uD86D\uDF39";
        log.info("Unicode 2B739 -> `{}`", oneWordUseTwoChar);
        then(oneWordUseTwoChar.length()).isEqualTo(2);
        then(oneWordUseTwoChar.toCharArray()).contains('\uD86D', '\uDF39');
        then(oneWordUseTwoChar.getBytes(UTF_8)).hasSize(4);
    }

    /**
     * 系统支持 Unicode, 每个中文算一个 char
     * <p>Java 中每个 char 占用 2 个 byte，如何表示 ? 中国：U+4E2DU+56FD</p>
     */
    @Test
    void given中文_whenUTF8_then() {
        // 你好，世界。！unicode          =>  \u4f60\u597d\uff0c\u4e16\u754c\u3002\uff01 共7个字符
        // 你好，世界。！UTF_8            =>  E4BDA0,E5A5BD,EFBC8C,E4B896,E7958C,E38082,EFBC81 共21个字节
        // 'Hello, World!\n' unicode  => 'Hello, World!\n'  共 14 个字符
        // 'Hello, World!\n' utf_8    => 48,65,6C,6C,6F,2C,20,57,6F,72,6C,64,21,0A 共14个字节
        String str = "Hello, World!\n你好，世界。！";
        then(str.length()).isEqualTo(21);
        char[] chars = str.toCharArray();
        then(chars.length).isEqualTo(str.length());
        //
        then(new String(chars)).isEqualTo(str);
        byte[] bytes = str.getBytes(UTF_8);
        then(bytes.length).isEqualTo(21 + 14);
    }


    @Test
    void given中文_whenWriteByteBuf_thenFailed() {
        ByteBuf buf = Unpooled.buffer();
        String str = "Hello, World!\n你好，世界。！";
        // 错误的用法
        // str.length 返回的值 char 类型的字符数量（21）
        buf.writeShort(str.length());
        // writeCharSequence 会写入 utf_8 编码后的字节数量（35）
        buf.writeCharSequence(str, UTF_8);
        // 读不出来字符串
        CharSequence actual = buf.readCharSequence(buf.readShort(), UTF_8);
        then(actual).isNotEqualTo(str);
    }

    @Test
    void given中文_whenWriteByteBuf_thenSuccess() {
        ByteBuf buf = Unpooled.buffer();
        String str = "Hello, World!\n你好，世界。！";
        // 返回 UTF_8 编码后的字节数组（35个字节）
        byte[] bytes = str.getBytes(UTF_8);
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
        CharSequence actual = buf.readCharSequence(buf.readShort(), UTF_8);
        then(actual).isEqualTo(str);
    }

    @Test
    void given中文_whenWriteByteBuf_thenSuccess2() {
        ByteBuf buf = Unpooled.buffer();
        String str = "Hello, World!\n你好，世界。！";
        int marked = buf.writerIndex();
        buf.writeShort(0);
        buf.writeCharSequence(str, UTF_8);
        int strUtf8Length = buf.writerIndex() - marked - 2;
        buf.setShort(marked, strUtf8Length);
        CharSequence actual = buf.readCharSequence(buf.readShort(), UTF_8);
        then(actual).isEqualTo(str);
    }

    @Test
    void given中文_whenBase64_then() {
        ByteBuf buf = Unpooled.buffer();
        String str = "Hello, World!\n你好，世界。！";
        int marked = buf.writerIndex();
        buf.writeShort(0);
        buf.writeCharSequence(str, UTF_8);
        int strUtf8Length = buf.writerIndex() - marked - 2;
        buf.setShort(marked, strUtf8Length);
        String base64Str = Base64.encode(buf).toString(UTF_8);
        ByteBuf base64Decoded = Base64.decode(Unpooled.copiedBuffer(base64Str, UTF_8));
        CharSequence actual = base64Decoded.readCharSequence(base64Decoded.readShort(), UTF_8);
        then(actual).isEqualTo(str);
    }


    @Test
    void given_whenResetReaderIndex2Times_then() {
        ByteBuf buf = Unpooled.copiedBuffer("Hello,World", UTF_8);
        then(buf.readByte()).isEqualTo((byte) 'H');
        int readIndex = buf.readerIndex();
        buf.markReaderIndex();
        then(buf.readByte()).isEqualTo((byte) 'e');
        // reset 1 time
        buf.resetReaderIndex();
        // reset 2 time
        buf.resetReaderIndex();
        then(buf.readerIndex()).isEqualTo(readIndex);
    }

}
