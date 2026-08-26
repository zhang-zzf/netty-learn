package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SUBSCRIPTION_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Subscribe extends ControlPacket {

    public static final byte BYTE_0 = (byte) 0x82;
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
        Subscribe ret = new Subscribe(BYTE_0, remainingLength, packetIdentifier, subscriptions);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException("Invalid packet");
        }
        return ret;
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
        if (this.byte0 != BYTE_0) {
            return false;
        }
        //  The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair.
        if (subscriptions.isEmpty()) {
            return false;
        }
        for (Subscription sub : subscriptions) {
            if (!sub.validate()) {
                return false;
            }
        }
        return true;
    }

    public List<Subscription> grantSubscription(List<Integer> grantedQos) {
        List<Subscription> ret = new ArrayList<>(subscriptions.size());
        for (int i = 0; i < subscriptions.size(); i++) {
            Integer qos = grantedQos.get(i);
            if (qos < 0x80) {
                ret.add(subscriptions.get(i).newWithQos(qos));
            }
        }
        return ret;
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

        public Subscription newWithQos(int qos) {
            return new Subscription(topicFilter, (byte) (qos & 0x03));
        }

        boolean validate() {
            return validateTopicFilter() && validateOptions();
        }

        boolean validateTopicFilter() {
            if (topicFilter == null || topicFilter.isEmpty()) {
                return false;
            }
            String[] levels = splitSlashSeparateStr(topicFilter);
            for (int i = 0; i < levels.length; i++) {
                String level = levels[i];
                if (level.contains("+")) {
                    if (!"+".equals(level)) {// + 必须整层只有 +，不能混杂其他字符
                        return false;
                    }
                }
                if (level.contains("#")) {
                    if (!"#".equals(level)) {// # 必须整层只有 #
                        return false;
                    }
                    if (i != levels.length - 1) {// # 必须是最后一层
                        return false;
                    }
                }
            }
            return true;
        }

        boolean validateOptions() {
            // The Server MUST treat a SUBSCRIBE packet as malformed and close the
            // Network Connection if any of Reserved bits in the payload are non-zero, or QoS is not 0,1 or 2
            if (qos() == 0x11) {
                return false;
            }
            if ((options & 0xFC) != 0) {
                return false;
            }
            return true;
        }

        public static class V50 extends Subscription {

            /**
             * Send retained messages at the time of the subscribe
             */
            public static final int RETAIN_HANDLING_SEND_RETAIN = 0;
            /**
             * Send retained messages at subscribe only if the subscription does not currently exist
             */
            public static final int RETAIN_HANDLING_SEND_IF_NEW = 1;
            /**
             * Do not send retained messages at the time of the subscribe
             */
            public static final int RETAIN_HANDLING_NOT_SEND = 2;
            public static final String $_SHARE = "$share/";

            final Integer identifier;// maybe null

            public V50(String topicFilter, byte options, Integer identifier) {
                super(topicFilter, options);
                this.identifier = identifier;
            }

            @Override
            public Subscription.V50 newWithQos(int qos) {
                byte options = (byte) (this.options & 0xFC | (qos & 0x03));
                return new Subscription.V50(topicFilter, options, identifier);
            }

            @Override
            boolean validateTopicFilter() {
                return super.validateTopicFilter()
                        && validateSharedSubscriptionTopicFilter();
            }

            private boolean validateSharedSubscriptionTopicFilter() {
                if (isShared()) {
                    String[] levels = splitSlashSeparateStr(topicFilter);
                    // "$share"/"group"/"filter"
                    if (levels.length < 3) {
                        return false;
                    }
                    if (!validateSharedGroupName(levels[1])) {
                        return false;
                    }
                    if (levels[2].isEmpty()) {
                        return false;
                    }
                    return true;
                }
                return true;
            }

            private boolean validateSharedGroupName(String group) {
                if (group == null || group.isEmpty()) {
                    return false;
                }
                if (group.contains(SINGLE_LEVEL_WILDCARD) || group.contains(MULTI_LEVEL_WILDCARD)) {
                    return false;
                }
                return true;
            }

            @Override
            boolean validateOptions() {
                if (qos() == 0x11) {
                    return false;
                }
                if (retainHandling() == 0x11) {
                    return false;
                }
                if ((options & 0xC0) != 0) {
                    return false;
                }
                return true;
            }

            public boolean isShared() {
                return topicFilter.startsWith($_SHARE);
            }

            public String sharedGroup() {
                if (isShared()) {
                    return splitSlashSeparateStr(topicFilter)[1];
                }
                throw new IllegalArgumentException();
            }

            public String sharedFilter() {
                if (isShared()) {
                    int index = topicFilter.indexOf('/', 7);
                    return topicFilter.substring(index + 1);
                }
                throw new IllegalArgumentException();
            }

            public static String fullTopicFilter(String group, String filter) {
                return $_SHARE + group + LEVEL_SEPARATOR + filter;
            }

            public Optional<Integer> identifier() {
                return Optional.ofNullable(identifier);
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
            Optional<Integer> subId = properties.subscriptionIdentifier();
            List<Subscription> subscriptions = new ArrayList<>();
            while (incoming.isReadable()) {
                String topic = readUTF8String(incoming);
                byte options = readByte(incoming);
                subscriptions.add(new Subscription.V50(topic, options, subId.orElse(null)));
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
                    && properties.validateIdentifier(allowedProperties);
        }

    }
}
