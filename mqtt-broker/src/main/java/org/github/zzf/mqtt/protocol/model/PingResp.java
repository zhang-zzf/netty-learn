package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PingResp extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xD0;

    public static PingResp from() {
        return new PingResp(_0_BYTE, 0x00);
    }

    private PingResp(byte byte0, int remainingLength) {
        super(byte0, remainingLength);
    }

    public static PingResp incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        return new PingResp(byte0, remainingLength);
    }
}
