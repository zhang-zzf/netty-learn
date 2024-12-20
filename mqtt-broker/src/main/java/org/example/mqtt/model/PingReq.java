package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PingReq extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xC0;

    PingReq(ByteBuf buf) {
        super(buf);
    }

    public PingReq() {
        super(_0_BYTE, 0);
    }

    @Override
    public boolean packetValidate() {
        return byte0 == _0_BYTE;
    }

}
