package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class Disconnect extends ControlPacket {

    public Disconnect(ByteBuf buf) {
        super(buf);
    }

    @Override
    public boolean packetValidate() {
        return _0Byte() == 0xE0;
    }

}
