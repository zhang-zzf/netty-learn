package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class SubAck extends ControlPacket {

    private short packetIdentifier;

    public SubAck(short packetIdentifier, List<Subscription> subscriptionList) {
        this.packetIdentifier = packetIdentifier;
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeByte(0x90);
        buf.writeByte(0x02 + 0x01 * subscriptionList.size());
        buf.writeShort(packetIdentifier);
        for (Subscription s : subscriptionList) {
            buf.writeByte(s.getQos());
        }
        this.setBuf(buf);
    }

}
