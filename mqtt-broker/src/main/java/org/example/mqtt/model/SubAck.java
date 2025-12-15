package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.example.mqtt.model.Subscribe.Subscription;

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

    SubAck(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
        this.subscriptions = new ArrayList<>();
        while (incoming.isReadable()) {
            this.subscriptions.add(new Subscription(null, incoming.readByte()));
        }
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
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
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(hexPId(packetIdentifier)).append(',');
        if (subscriptions != null) {
            sb.append("\"subscriptions\":");
            if (!(subscriptions).isEmpty()) {
                sb.append("[");
                final int listSize = (subscriptions).size();
                for (int i = 0; i < listSize; i++) {
                    final Object listValue = (subscriptions).get(i);
                    if (listValue instanceof CharSequence) {
                        sb.append("\"").append(Objects.toString(listValue, "")).append("\"");
                    }
                    else {
                        sb.append(Objects.toString(listValue, ""));
                    }
                    if (i < listSize - 1) {
                        sb.append(",");
                    }
                    else {
                        sb.append("]");
                    }
                }
            }
            else {
                sb.append("[]");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
