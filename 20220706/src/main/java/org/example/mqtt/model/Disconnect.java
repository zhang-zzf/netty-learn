package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Disconnect extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xE0;

    public Disconnect(ByteBuf buf) {
        super(buf);
    }

    private Disconnect() {
        super(_0_BYTE, 0x00);
    }

    public static Disconnect from() {
        return new Disconnect();
    }

    @Override
    public ByteBuf toByteBuf() {
        return fixedHeaderByteBuf();
    }

    @Override
    public boolean packetValidate() {
        return this.byte0 == _0_BYTE;
    }

    @Override
    protected void initPacket() {
        // noop
    }

}
