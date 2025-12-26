package org.github.zzf.mqtt.protocol.model;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Subscribe extends ControlPacket {

    private final short packetIdentifier;
    private final List<Subscription> subscriptions;

    Subscribe(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
        this.subscriptions = new ArrayList<>();
        while (incoming.isReadable()) {
            String topic = incoming.readCharSequence(incoming.readShort(), UTF_8).toString();
            byte qos = incoming.readByte();
            // todo TopicFilter rule check
            this.subscriptions.add(new Subscription(topic, qos));
        }
    }

    public static Subscribe from(List<Subscription> subscriptions) {
        return from((short) 0, subscriptions);
    }

    public static Subscribe from(short packetIdentifier,
            List<Subscription> subscriptions) {
        if (subscriptions.isEmpty()) {
            throw new IllegalArgumentException();
        }
        int remainingLength = 2;
        for (Subscription s : subscriptions) {
            remainingLength += (2 + s.topicFilter().getBytes(UTF_8).length + 1);
        }
        return new Subscribe((byte) 0x82, remainingLength, packetIdentifier, subscriptions);
    }

    private Subscribe(byte _0Byte,
            int remainingLength,
            short packetIdentifier,
            List<Subscription> subscriptions) {
        super(_0Byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.subscriptions = subscriptions;
        if (!packetValidate()) {
            throw new IllegalArgumentException("Invalid packet");
        }
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        buf.writeShort(packetIdentifier);
        for (Subscription s : subscriptions) {
            byte[] bytes = s.topicFilter().getBytes(UTF_8);
            buf.writeShort(bytes.length);
            buf.writeBytes(bytes);
            buf.writeByte(s.qos());
        }
        return buf;
    }

    public List<Subscription> subscriptions() {
        return this.subscriptions;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public boolean packetValidate() {
        // Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control Packet are reserved and MUST be set to
        // 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
        if (this.byte0 != (byte) 0x82) {
            return false;
        }
        //  The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair.
        if (subscriptions.isEmpty()) {
            return false;
        }
        for (Subscription sub : subscriptions) {
            int qos = sub.qos();
            // The Server MUST treat a SUBSCRIBE packet as malformed and close the
            // Network Connection if any of Reserved bits in the payload are non-zero, or QoS is not 0,1 or 2
            if ((qos & 0xFC) != 0) {
                return false;
            }
            // todo TopicFilter check
            if (!topicFilterValidate(sub.topicFilter)) {
                return false;
            }
            if (qos == 0 || qos == 1 || qos == 2) {
                continue;
            }
            else {
                return false;
            }
        }
        return true;
    }

    static boolean topicFilterValidate(String topicFilter) {
        if (topicFilter == null) {
            return false;
        }
        int idx;
        if ((idx = topicFilter.indexOf("#")) != -1) {
            if (idx != topicFilter.length() - 1) {
                // sport/tennis/#/ranking is not valid
                return false;
            }
            if (topicFilter.length() > 1 && topicFilter.charAt(idx - 1) != '/') {
                // example "#" is valid
                // example “sport/tennis#” is not valid
                return false;
            }
        }
        if ((idx = topicFilter.indexOf("+")) != -1) {
            if (topicFilter.length() == 1) {
                return true;
            }
            if (topicFilter.charAt(idx - 1) != '/') {
                return false;
            }
            if (idx + 1 < topicFilter.length() && topicFilter.charAt(idx + 1) != '/') {
                return false;
            }
        }
        return true;
    }

    public static class Subscription {

        private final String topicFilter;
        private final int qos;

        public Subscription(String topicFilter,
                int qos) {
            this.topicFilter = topicFilter;
            this.qos = qos;
        }

        public String topicFilter() {
            return this.topicFilter;
        }

        public int qos() {
            return this.qos;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Subscription that = (Subscription) o;
            return topicFilter.equals(that.topicFilter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicFilter);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (topicFilter != null) {
                sb.append("\"topicFilter\":\"").append(topicFilter).append('\"').append(',');
            }
            sb.append("\"qos\":").append(qos).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }
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
