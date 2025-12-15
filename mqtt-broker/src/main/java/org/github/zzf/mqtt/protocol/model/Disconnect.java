package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class Disconnect extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xE0;

    Disconnect(ByteBuf buf) {
        super(buf);
    }

    public Disconnect() {
        super(_0_BYTE, 0x00);
    }

    @Override
    public boolean packetValidate() {
        return this.byte0 == _0_BYTE;
    }

}
