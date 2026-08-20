package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.CONTENT_TYPE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.CORRELATION_DATA;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.MESSAGE_EXPIRY_INTERVAL;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.PAYLOAD_FORMAT_INDICATOR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RESPONSE_TOPIC;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SUBSCRIPTION_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.TOPIC_ALIAS;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Publish extends ControlPacket {

    public static final int AT_MOST_ONCE = 0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE = 2;
    public static final short NO_PACKET_IDENTIFIER = 0;
    public static final String META_P_RECEIVE_NANO = "p_receive_nano";
    /**
     * Publish Receive time
     */
    public static final String META_P_RECEIVE_MILLIS = "p_receive_millis";
    public static final String META_P_SOURCE = "p_source";
    public static final String META_P_SOURCE_BROKER = "broker";
    public static final String META_NM_WRAP = "nm_wrap";
    public static final String META_NM_RECEIVE = "nm_receive";

    final String topicName;
    final short packetIdentifier;
    final ByteBuf payload;
    /**
     * not protocol field
     * <p>just for metric usage</p>
     * <p>Client or Broker 接受到 Publish 的时间</p>
     * <p>default to 0</p>
     */
    private Map<String, Object> meta;

    Publish(byte byte0, int remainingLength,
            String topicName, short packetIdentifier,
            ByteBuf payload) {
        super(byte0, remainingLength);
        this.topicName = topicName;
        this.packetIdentifier = packetIdentifier;
        this.payload = payload;
        initMetricMetaData();
    }

    static Publish incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        String topicName = readUTF8String(incoming);
        short packetIdentifier = needAck(qos(byte0)) ? readPacketIdentifier(incoming) : 0;
        // core: zero-copy
        ByteBuf payload = incoming.readSlice(incoming.readableBytes());
        return new Publish(byte0, remainingLength,
                topicName, packetIdentifier,
                payload);
    }

    public static Publish outgoing(int qos,
            String topicName,
            ByteBuf payload) {
        return outgoing(false, qos, false, topicName, (short) 0, payload);
    }

    public static Publish outgoing(boolean retain, int qos, boolean dup,
            String topicName, short packetIdentifier,
            ByteBuf payload) {
        byte _0byte = build_0Byte(retain, qos, dup);
        int packetIdentifierLength = needAck(qos) ? 2 : 0;
        // remainingLength field
        int remainingLength = calcUTF8StringLength(topicName) + packetIdentifierLength + payload.readableBytes();
        Publish ret = new Publish(_0byte, remainingLength,
                topicName, packetIdentifier,
                payload);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    /**
     * whether the qos need receiver ack
     *
     * @return true / false;
     */
    public static boolean needAck(int qos) {
        return qos == AT_LEAST_ONCE || qos == EXACTLY_ONCE;
    }

    static byte build_0Byte(boolean retain,
            int qos,
            boolean dup) {
        byte _0Byte = 0x30;
        if (retain) {
            _0Byte |= 0x01;
        }
        _0Byte |= (byte) (qos << 1);
        if (dup) {
            _0Byte |= 0x08;
        }
        return _0Byte;
    }

    public static int qos(byte byte0) {
        return (byte0 & 0x06) >> 1;
    }

    private void initMetricMetaData() {
        // metric 多线程 同步 性能是否存在问题？
        // 线程封闭，无多线程同步
        addMeta(META_P_RECEIVE_NANO, System.nanoTime());
        addMeta(META_P_RECEIVE_MILLIS, System.currentTimeMillis());
    }

    @Override
    public ByteBuf toByteBuf() {
        // fixed header
        ByteBuf fixedHeader = fixedHeaderByteBuf();
        // variable header
        ByteBuf varHeader = varHeaderByteBuf();
        // the CompositeBuffer will be released by netty
        return compositeBuffer()
                .addComponents(true, fixedHeader, varHeader, payload);
    }

    protected ByteBuf fixedHeaderByteBuf() {
        // use direct buf will optimize netty zero-copy when write to Channel
        /** {@link Publish#toByteBuf()} */
        /** {@link AbstractNioByteChannel#filterOutboundMessage(Object)} */
        int fixedHeaderLength = 1 + variableByteIntegerLength(remainingLength);
        ByteBuf buf = directBuffer(fixedHeaderLength);
        writeByte(buf, byte0);
        // remainingLength field
        writeVariableByteInteger(buf, remainingLength);
        return buf;
    }


    protected ByteBuf varHeaderByteBuf() {
        int variableHeaderLength = remainingLength - payload.readableBytes();
        ByteBuf varHeader = directBuffer(variableHeaderLength);
        writeUTF8String(varHeader, topicName);
        if (needAck()) {
            writeTwoByteInteger(varHeader, packetIdentifier);
        }
        return varHeader;
    }

    /**
     * whether the packet need receiver ack
     *
     * @return true / false;
     */
    public boolean needAck() {
        return needAck(qos());
    }

    @Override
    protected boolean packetValidate() {
        // The DUP flag MUST be set to 0 for all QoS 0 messages
        if (qos() == 0 && dup()) {
            return false;
        }
        if ((qos() & 0x03) == 0x03) {
            return false;
        }
        // topicName 校验 forward 的 Publish topicName 可以包含 topicFilter
        if (!validateTopicName()) {
            return false;
        }
        return super.packetValidate();
    }

    public boolean validateTopicName() {
        return topicName != null && !topicName.isEmpty();
    }

    public boolean dup() {
        return (byte0 & 0x08) != 0;
    }

    public int qos() {
        return qos(byte0);
    }

    public boolean retainFlag() {
        return (byte0 & 0x01) != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Publish publish = (Publish) o;
        return packetIdentifier == publish.packetIdentifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetIdentifier);
    }

    public boolean atLeastOnce() {
        return qos() == AT_LEAST_ONCE;
    }

    public boolean exactlyOnce() {
        return qos() == EXACTLY_ONCE;
    }

    public boolean atMostOnce() {
        return qos() == AT_MOST_ONCE;
    }

    public ByteBuf payload() {
        return this.payload;
    }

    public String topicName() {
        return this.topicName;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"PUBLISH\",");
        sb.append("\"packetIdentifier\":\"").append(pId()).append("\",");
        if (topicName != null) {
            sb.append("\"topicName\":\"").append(topicName).append('\"').append(',');
        }
        sb.append("\"qos\":").append(qos()).append(",");
        sb.append("\"dup\":").append(dup()).append(",");
        sb.append("\"retain\":").append(retainFlag()).append(",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

    public Map<String, Object> meta() {
        return meta;
    }

    public void addMeta(String n,
            Object v) {
        if (meta == null) {
            meta = new HashMap<>(4);
        }
        meta.put(n, v);
    }

    private void copyMeta(Publish origin) {
        meta = origin.meta;
    }

    public static class V50 extends Publish {

        final long timestamp = System.currentTimeMillis();

        // If there are no properties, this MUST be indicated by including a Property Length of zero
        final Properties properties;

        public static V50 outgoing(boolean retain, int qos, boolean dup,
                String topicName, short packetIdentifier, Properties properties,
                ByteBuf payload) {
            byte _0byte = build_0Byte(retain, qos, dup);
            // remainingLength field
            int remainingLength = calcUTF8StringLength(topicName)
                    + (needAck(qos) ? 2 : 0)  // packetIdentifierLength
                    + calcPropertiesLength(properties)
                    + payload.readableBytes();
            V50 ret = new V50(_0byte, remainingLength,
                    topicName, packetIdentifier, properties,
                    payload);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return ret;
        }

        V50(byte byte0, int remainingLength,
                String topicName, short packetIdentifier, Properties properties,
                ByteBuf payload) {
            super(byte0, remainingLength, topicName, packetIdentifier, payload);
            this.properties = properties;
        }

        static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            String topicName = readUTF8String(incoming);
            short packetIdentifier = needAck(qos(byte0)) ? readPacketIdentifier(incoming) : 0;
            // core: zero-copy
            Properties properties = readProperties(incoming);
            // core: zero-copy
            ByteBuf payload = incoming.readSlice(incoming.readableBytes());
            return new V50(byte0, remainingLength,
                    topicName, packetIdentifier, properties,
                    payload);
        }

        public static V50 updateTopicName(V50 origin, String topicName) {
            if (!topicName.isEmpty()) {
                throw new MalformedPacketException();
            }
            return new V50(origin.byte0, origin.remainingLength + ControlPacket.UTF8StringLength(topicName),
                    topicName, origin.packetIdentifier, origin.properties,
                    origin.payload);
        }

        public Properties properties() {
            return this.properties;
        }

        @Override
        protected boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties);
        }

        @Override
        protected ByteBuf varHeaderByteBuf() {
            ByteBuf buf = super.varHeaderByteBuf();
            writeProperties(buf, properties);
            return buf;
        }

        final Set<Integer> allowedProperties = Set.of(
                PAYLOAD_FORMAT_INDICATOR,
                MESSAGE_EXPIRY_INTERVAL,
                TOPIC_ALIAS,
                RESPONSE_TOPIC,
                CORRELATION_DATA,
                USER_PROPERTY,
                SUBSCRIPTION_IDENTIFIER,
                CONTENT_TYPE
        );

        public long timestamp() {
            return timestamp;
        }
    }
}
