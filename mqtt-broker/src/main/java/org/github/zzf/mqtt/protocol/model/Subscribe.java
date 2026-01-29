package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SUBSCRIPTION_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Subscribe extends ControlPacket {

    final short packetIdentifier;
    final List<Subscription> subscriptions;

    private Subscribe(byte _0Byte,
            int remainingLength,
            short packetIdentifier,
            List<Subscription> subscriptions) {
        super(_0Byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.subscriptions = subscriptions;
    }

    public static Subscribe incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        List<Subscription> subscriptions = new ArrayList<>();
        while (incoming.isReadable()) {
            String topic = readUTF8String(incoming);
            byte options = readByte(incoming);
            // todo TopicFilter rule check
            subscriptions.add(new Subscription(topic, options));
        }
        return new Subscribe(byte0, remainingLength, packetIdentifier, subscriptions);
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
            remainingLength += (calcUTF8StringLength(s.topicFilter) + 1);
        }
        Subscribe ret = new Subscribe((byte) 0x82, remainingLength, packetIdentifier, subscriptions);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException("Invalid packet");
        }
        return ret;
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
            return idx + 1 >= topicFilter.length() || topicFilter.charAt(idx + 1) == '/';
        }
        return true;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        for (Subscription s : subscriptions) {
            writeUTF8String(buf, s.topicFilter);
            writeByte(buf, s.options);
        }
        return buf;
    }

    private ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
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
            if (qos != 0 && qos != 1 && qos != 2) {
                return false;
            }
        }
        return true;
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

    public static class Subscription {

        final String topicFilter;
        final byte options;

        public Subscription(String topicFilter, byte options) {
            this.topicFilter = topicFilter;
            this.options = options;
        }

        public String topicFilter() {
            return this.topicFilter;
        }

        public int qos() {
            return this.options & 0x03;
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
            sb.append("\"options\":").append(options).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

        public static class V50 extends Subscription {

            public V50(String topicFilter, byte options) {
                super(topicFilter, options);
            }

            public boolean noLocal() {
                return (this.options & 0x04) != 0;
            }

            public boolean retainAsPublished() {
                return (this.options & 0x08) != 0;
            }

            public int retainHandling() {
                return (this.options & 0x30) >> 4;
            }
        }
    }

    public static class V50 extends Subscribe {
        // If there are no properties, this MUST be indicated by including a Property Length of zero
        final Properties properties;
        final Set<Integer> allowedProperties = Set.of(SUBSCRIPTION_IDENTIFIER, USER_PROPERTY);

        V50(byte byte0, int remainingLength,
                short packetIdentifier, Properties properties,
                List<Subscription> subscriptions) {
            super(byte0, remainingLength, packetIdentifier, subscriptions);
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            Properties properties = readProperties(incoming);
            List<Subscription> subscriptions = new ArrayList<>();
            while (incoming.isReadable()) {
                String topic = readUTF8String(incoming);
                byte options = readByte(incoming);
                // todo TopicFilter rule check
                subscriptions.add(new Subscription(topic, options));
            }
            return new V50(byte0, remainingLength,
                    packetIdentifier, properties,
                    subscriptions);
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toPacketByteBuf();
            writeTwoByteInteger(buf, packetIdentifier);
            writeProperties(buf, properties);
            for (Subscription s : subscriptions) {
                writeUTF8String(buf, s.topicFilter);
                writeByte(buf, s.options);
            }
            return buf;
        }

        public Properties properties() {
            return this.properties;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties)
                    && validateSubscriptionOption();
        }

        private boolean validateSubscriptionOption() {
            for (Subscription s : subscriptions) {
                if (s instanceof Subscription.V50 v50) {
                    if (v50.retainHandling() == 3) {// It is a Protocol Error to send a Retain Handling value of 3
                        return false;
                    }
                    // Bits 6 and 7 of the Subscription Options byte are reserved for future us
                    if ((s.options & 0xC0) != 0) {
                        return false;
                    }
                }
                else {
                    return false;
                }
            }
            return true;
        }

    }
}
