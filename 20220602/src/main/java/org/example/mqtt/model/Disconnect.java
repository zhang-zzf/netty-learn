package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Disconnect extends ControlPacket {

    public Disconnect(ByteBuf buf) {
        super(buf);
    }

    @Override
    public boolean packetValidate() {
        return this._0byte == 0xE0;
    }

}
