package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PingReq extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xC0;

    public static PingReq from() {
        return new PingReq(_0_BYTE, 0);
    }

    private PingReq(byte byte0, int remainingLength) {
        super(byte0, remainingLength);
    }

    public static PingReq incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        return new PingReq(byte0, remainingLength);
    }

    @Override
    public boolean packetValidate() {
        return byte0 == _0_BYTE;
    }

}
