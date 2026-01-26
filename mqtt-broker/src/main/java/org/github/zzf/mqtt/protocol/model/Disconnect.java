package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class Disconnect extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xE0;

    static Disconnect incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        return new Disconnect(byte0, remainingLength);
    }

    private Disconnect(byte byte0, int remainingLength) {
        super(byte0, remainingLength);
    }

    public static Disconnect from() {
        return new Disconnect(_0_BYTE, 0x00);
    }

    @Override
    public boolean packetValidate() {
        return this.byte0 == _0_BYTE;
    }

}
