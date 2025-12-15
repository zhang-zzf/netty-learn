package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PingResp extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xD0;

    PingResp(ByteBuf packet) {
        super(packet);
    }

    public PingResp() {
        super(_0_BYTE, 0x00);
    }

}
