package org.example.mqtt.model;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class Publish extends ControlPacket {

    public static final int AT_MOST_ONCE = 0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE = 2;
    public static final short NO_PACKET_IDENTIFIER = 0;

    private String topicName;
    private short packetIdentifier;
    private final ByteBuf payload;
    /**
     * share payload with another Publish
     */
    private final boolean sharedPayload;

    private int variableHeaderLength = 0;

    /**
     * <pre>
     *      inbound packet convert to model
     *      the incoming Publish need be used ad a PublishInbound, so the constructor is package-access.
     * </pre>
     *
     * @param incoming inbound packet
     */
    Publish(ByteBuf incoming) {
        super(incoming);
        short topicNameLng = incoming.readShort();
        this.topicName = incoming.readCharSequence(topicNameLng, UTF_8).toString();
        variableHeaderLength += 2 + topicNameLng;
        if (needAck()) {
            this.packetIdentifier = incoming.readShort();
            variableHeaderLength += 2;
        }
        // core : slice
        this.payload = incoming.readSlice(incoming.readableBytes());
        this.sharedPayload = true;
        initMetricMetaData();
    }

    private void initMetricMetaData() {
        // metric 多线程 同步 性能是否存在问题？
        // 线程封闭，无多线程同步
        addMeta(META_P_RECEIVE_NANO, System.nanoTime());
        addMeta(META_P_RECEIVE_MILLIS, System.currentTimeMillis());
    }

    /**
     * outgoing Publish Message
     */
    private Publish(byte _0byte, int remainingLength, short packetIdentifier, ByteBuf payload, String topicName,
        boolean sharedPayload) {
        super(_0byte, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.topicName = topicName;
        this.payload = payload;
        this.variableHeaderLength = remainingLength - payload.readableBytes();
        this.sharedPayload = sharedPayload;
        if (!packetValidate()) {
            throw new IllegalArgumentException("Invalid packet");
        }
    }

    /**
     * Publish to Publish use Zero-Copy of ByteBuf for payload
     *
     * @param origin           source
     * @param packetIdentifier packet ID
     * @return a Publish Packet that have the save data as source
     */
    public static Publish outgoing(Publish origin, String topicName, byte qos, short packetIdentifier) {
        Publish ret = outgoing(origin.retainFlag(), qos, false, topicName, packetIdentifier, origin.payload, true);
        // metric meta
        ret.copyMeta(origin);
        return ret;
    }

    /**
     * Publish to Publish use Zero-Copy of ByteBuf for payload
     *
     * @param origin           source
     * @param packetIdentifier packet ID
     * @return a new Publish Packet that have the save data as source
     */
    public static Publish outgoing(Publish origin, boolean dup, byte qos, short packetIdentifier) {
        return outgoing(origin.retainFlag(), qos, dup, origin.topicName, packetIdentifier, origin.payload, true);
    }

    public static Publish outgoing(int qos, String topicName, ByteBuf payload) {
        return outgoing(false, qos, false, topicName, (short) 0, payload, false);
    }

    public static Publish outgoing(boolean retain, int qos, boolean dup,
        String topicName, short packetIdentifier, ByteBuf payload, boolean sharedPayload) {
        byte _0byte = build_0Byte(retain, qos, dup);
        int topicLength = topicName.getBytes(UTF_8).length + 2;
        int packetIdentifierLength = needAck(qos) ? 2 : 0;
        // remainingLength field
        int remainingLength = topicLength + packetIdentifierLength + payload.readableBytes();
        return new Publish(_0byte, remainingLength, packetIdentifier, payload, topicName, sharedPayload);
    }

    @Override
    public ByteBuf toByteBuf() {
        // fixed header
        ByteBuf fixedHeader = fixedHeaderByteBuf();
        // variable header
        ByteBuf varHeader = directBuffer(variableHeaderLength);
        byte[] topicNameBytes = topicName.getBytes(UTF_8);
        varHeader.writeShort(topicNameBytes.length);
        varHeader.writeBytes(topicNameBytes);
        if (needAck()) {
            varHeader.writeShort(packetIdentifier);
        }
        // packet
        if (sharedPayload) {
            log.info("Publish({}) shared payload retained ", pId());
            payload.retain();
        }
        // the CompositeBuffer will be released by netty
        return Unpooled.compositeBuffer()
            .addComponents(true, fixedHeader, varHeader, payload);
    }

    /**
     * whether the qos need receiver ack
     *
     * @return true / false;
     */
    public static boolean needAck(int qos) {
        return qos == AT_LEAST_ONCE || qos == EXACTLY_ONCE;
    }

    /**
     * whether the packet need receiver ack
     *
     * @return true / false;
     */
    public boolean needAck() {
        return needAck(qos());
    }

    static byte build_0Byte(boolean retain, int qos, boolean dup) {
        byte _0Byte = 0x30;
        if (retain) {
            _0Byte |= 0x01;
        }
        _0Byte |= qos << 1;
        if (dup) {
            _0Byte |= 0x08;
        }
        return _0Byte;
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
        return super.packetValidate();
    }

    public boolean dup() {
        return (byte0 & 0x08) != 0;
    }

    public int qos() {
        return (this.byte0 & 0x06) >> 1;
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

    /**
     * update packetIdentifier
     *
     * @param packetIdentifier the new id
     * @return this
     */
    public Publish packetIdentifier(short packetIdentifier) {
        this.packetIdentifier = packetIdentifier;
        return this;
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

    public Publish topicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public Publish qos(byte qos) {
        this.updateQos(qos);
        return this;
    }

    private void updateQos(byte qos) {
        int clearQoS = this.byte0 & 0xF9;
        this.byte0 = (byte) (clearQoS | (qos << 1));
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

    public Publish dup(boolean dup) {
        if (dup) {
            this.byte0 |= 0x08;
        }
        else {
            this.byte0 &= 0xF7;
        }
        return this;
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

    /**
     * create a new Publish Packet that use Unpooled ByteBuf payload
     *
     * @return the copied Publish Packet
     */
    public Publish copy() {
        ByteBuf payload = Unpooled.copiedBuffer(payload());
        return Publish.outgoing(retainFlag(), (byte) qos(), dup(), topicName(), (short) 0, payload, false);
    }

    public Publish retainFlag(boolean flag) {
        if (flag) {
            byte0 |= 0x01;
        }
        else {
            byte0 &= 0xFE;
        }
        return this;
    }

    /**
     * not protocol field
     * <p>just for metric usage</p>
     * <p>Client or Broker 接受到 Publish 的时间</p>
     * <p>default to 0</p>
     */
    private Map<String, Object> meta;

    public static final String META_P_RECEIVE_NANO = "p_receive_nano";
    /**
     * Publish Receive time
     */
    public static final String META_P_RECEIVE_MILLIS = "p_receive_millis";
    public static final String META_P_SOURCE = "p_source";
    public static final String META_P_SOURCE_BROKER = "broker";
    public static final String META_NM_WRAP = "nm_wrap";
    public static final String META_NM_RECEIVE = "nm_receive";

    @Nullable
    public Map<String, Object> meta() {
        return meta;
    }

    public void addMeta(String n, Object v) {
        if (meta == null) {
            meta = new HashMap<>(4);
        }
        meta.put(n, v);
    }

    private void copyMeta(Publish origin) {
        meta = origin.meta;
    }

}
