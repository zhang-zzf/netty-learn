package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

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
