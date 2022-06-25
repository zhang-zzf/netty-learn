package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.util.List;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class SubAck extends ControlPacket {

    @Getter
    private final short packetIdentifier;
    @Getter
    private final List<Subscription> subscriptionList;

    public SubAck(short packetIdentifier, List<Subscription> subscriptionList) {
        super((byte) 0x02, 0x01 * subscriptionList.size());
        this.packetIdentifier = packetIdentifier;
        this.subscriptionList = subscriptionList;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        for (Subscription s : subscriptionList) {
            buf.writeByte(s.getQos());
        }
        return buf;

    }

}
