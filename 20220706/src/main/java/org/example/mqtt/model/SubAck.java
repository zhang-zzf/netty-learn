package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class SubAck extends ControlPacket {

    private short packetIdentifier;
    private List<Subscribe.Subscription> subscriptions;

    public static SubAck from(short packetIdentifier, List<Subscribe.Subscription> subscriptions) {
        int rl = 2 + subscriptions.size();
        return new SubAck(rl, packetIdentifier, subscriptions);
    }

    public static SubAck from(List<Subscribe.Subscription> subscriptions) {
        return from((short) 0, subscriptions);
    }

    private SubAck(int remainingLength, short packetIdentifier, List<Subscribe.Subscription> subscriptions) {
        super(SUBACK, remainingLength);
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

    public List<Subscribe.Subscription> subscriptions() {
        return this.subscriptions;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packetIdentifier\":").append(packetIdentifier).append(',');
        if (subscriptions != null) {
            sb.append("\"subscriptions\":");
            if (!(subscriptions).isEmpty()) {
                sb.append("[");
                final int listSize = (subscriptions).size();
                for (int i = 0; i < listSize; i++) {
                    final Object listValue = (subscriptions).get(i);
                    if (listValue instanceof CharSequence) {
                        sb.append("\"").append(Objects.toString(listValue, "")).append("\"");
                    } else {
                        sb.append(Objects.toString(listValue, ""));
                    }
                    if (i < listSize - 1) {
                        sb.append(",");
                    } else {
                        sb.append("]");
                    }
                }
            } else {
                sb.append("[]");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
