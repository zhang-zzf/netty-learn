package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Unsubscribe extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xA2;
    private short packetIdentifier;
    private List<Subscribe.Subscription> subscriptions;

    public List<Subscribe.Subscription> subscriptions() {
        return this.subscriptions;
    }

    Unsubscribe(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
        this.subscriptions = new ArrayList<>();
        while (incoming.isReadable()) {
            String topic = incoming.readCharSequence(incoming.readShort(), UTF_8).toString();
            this.subscriptions.add(new Subscribe.Subscription(topic, 0));
        }
    }

    public static Unsubscribe from(List<Subscribe.Subscription> subscriptions) {
        return from((short) 0, subscriptions);
    }

    public static Unsubscribe from(short packetIdentifier, List<Subscribe.Subscription> subscriptions) {
        if (subscriptions.isEmpty()) {
            throw new IllegalArgumentException();
        }
        int remainingLength = 2;
        for (Subscribe.Subscription s : subscriptions) {
            remainingLength += (2 + s.topicFilter().getBytes(UTF_8).length);
        }
        return new Unsubscribe(_0_BYTE, remainingLength, packetIdentifier, subscriptions);
    }

    private Unsubscribe(byte _0Byte, int remainingLength,
                        short packetIdentifier,
                        List<Subscribe.Subscription> subscriptions) {
        super(_0Byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.subscriptions = subscriptions;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf header = fixedHeaderByteBuf();
        header.writeShort(packetIdentifier);
        ByteBuf payload = Unpooled.buffer(this.remainingLength);
        for (Subscribe.Subscription s : subscriptions) {
            byte[] bytes = s.topicFilter().getBytes(UTF_8);
            payload.writeShort(bytes.length);
            payload.writeBytes(bytes);
        }
        return Unpooled.compositeBuffer().addComponents(true, header, payload);
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public boolean packetValidate() {
        // Bits 3,2,1 and 0 of the fixed header of the UNSUBSCRIBE Control Packet are reserved and MUST be set to
        // 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
        if (this.byte0 != _0_BYTE) {
            return false;
        }
        //  The payload of a UNSUBSCRIBE packet MUST contain at least one Topic Filter.
        return !subscriptions.isEmpty();
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

    public Unsubscribe packetIdentifier(short packetIdentifier) {
        this.packetIdentifier = packetIdentifier;
        return this;
    }

}
