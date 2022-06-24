package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class PingReq extends ControlPacket {

    public PingReq(ByteBuf buf) {
        super(buf);
    }

    @Override
    public boolean packetValidate() {
        return _0Byte() == 0xC0;
    }

}
