package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class SubAck extends ControlPacket {

    private short packetIdentifier;
    private List<Subscribe.Subscription> subscriptions;

    public static SubAck from(short packetIdentifier, List<Subscribe.Subscription> subscriptions) {
        return new SubAck(packetIdentifier, subscriptions);
    }

    public static SubAck from(List<Subscribe.Subscription> subscriptions) {
        return from((short) 0, subscriptions);
    }

    private SubAck(short packetIdentifier, List<Subscribe.Subscription> subscriptions) {
        super(SUBACK, 0x01 * subscriptions.size());
        this.packetIdentifier = packetIdentifier;
        this.subscriptions = subscriptions;
    }

    public SubAck(ByteBuf packet) {
        super(packet);
    }

    @Override
    protected void initPacket() {
        ByteBuf _buf = _buf();
        this.packetIdentifier = _buf.readShort();
        this.subscriptions = new ArrayList<>();
        while (_buf.isReadable()) {
            this.subscriptions.add(new Subscribe.Subscription(null, _buf.readByte()));
        }
    }

    @Override
    public ByteBuf toByteBuf() {
        packetValidate();
        ByteBuf buf = fixedHeaderByteBuf();
        buf.writeShort(packetIdentifier);
        for (Subscribe.Subscription s : subscriptions) {
            buf.writeByte(s.qos());
        }
        return buf;
    }

    public List<Subscribe.Subscription> subscriptionList() {
        return this.subscriptions;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

}
