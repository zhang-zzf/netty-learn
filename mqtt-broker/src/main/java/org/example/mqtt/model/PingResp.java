package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PingResp extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xD0;

    PingResp(ByteBuf packet) {
        super(packet);
    }

    public PingResp() {
        super(_0_BYTE, 0x00);
    }

}
