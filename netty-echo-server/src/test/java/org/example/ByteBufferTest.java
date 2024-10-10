package org.example;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/02
 */
class ByteBufferTest {

    /**
     * Creates a new big-endian Java direct buffer
     */
    @Test
    void givenByteBuf_whenCreateHeapByteBuf_then() {
        ByteBuffer buf = ByteBuffer.allocateDirect(10 * 1024 * 1024);
        // DirectBuffer dd = (DirectBuffer) buf;
        buf = null;
        System.gc();
    }


    @SneakyThrows
    @Test void givenDirectBuffer_whenQueryUsage_then() {
        MBeanServer mbs = ManagementFactory. getPlatformMBeanServer() ;
        ObjectName objectName = new ObjectName("java.nio:type=BufferPool,name=direct" ) ;
        MBeanInfo info = mbs.getMBeanInfo(objectName) ;
        for(MBeanAttributeInfo i : info.getAttributes()) {
            System.out .println(i.getName() + ":" + mbs.getAttribute(objectName , i.getName()));
        }
    }
}