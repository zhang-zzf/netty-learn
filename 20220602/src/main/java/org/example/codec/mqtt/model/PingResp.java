package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class PingResp extends ControlPacket {

    public PingResp() {
        ByteBuf buf = Unpooled.buffer(2);
        buf.writeByte(0xD0);
        buf.writeByte(0x00);
        this.setBuf(buf);
    }

}
