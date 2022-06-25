package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class PingReq extends ControlPacket {

    public PingReq(ByteBuf buf) {
        super(buf);
    }

    @Override
    public boolean packetValidate() {
        return _0byte == 0xC0;
    }

}
