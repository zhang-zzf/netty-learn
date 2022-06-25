package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PingResp extends ControlPacket {

    public PingResp() {
        super((byte) 0xD0, 0x00);
    }

    @Override
    public ByteBuf toByteBuf() {
        return fixedHeaderByteBuf();
    }

}
