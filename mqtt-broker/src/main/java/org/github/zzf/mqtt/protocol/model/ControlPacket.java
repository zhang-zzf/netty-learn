package org.github.zzf.mqtt.protocol.model;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-17
 */
@Slf4j
public abstract class ControlPacket {

    public static final int _0_BYTE_LENGTH = 1;
    public static final int MIN_PACKET_LENGTH = 2;

    public static final byte CONNECT = 0x10;
    public static final byte CONNACK = 0x20;
    public static final byte PUBLISH = 0x30;
    public static final byte PUBACK = 0x40;
    public static final byte PUBREC = 0x50;
    public static final byte PUBREL = 0x60;
    public static final byte PUBCOMP = 0x70;
    public static final byte SUBSCRIBE = (byte) 0x80;
    public static final byte SUBACK = (byte) 0x90;
    public static final byte UNSUBSCRIBE = (byte) 0xA0;
    public static final byte UNSUBACK = (byte) 0xB0;
    public static final byte PINGREQ = (byte) 0xC0;
    public static final byte PINGRESP = (byte) 0xD0;
    public static final byte DISCONNECT = (byte) 0xE0;

    public static final int INCOMPLETE_PACKET = -1;
    public static final String MULTI_LEVEL_WILDCARD = "#";
    public static final String SINGLE_LEVEL_WILDCARD = "+";
    // 0 0x00 The message is accepted. Publication of the QoS 1 message proceeds.
    public static final byte REASON_CODE_SUCCESS = 0x00;
    // 16 0x10 No matching subscribers - The message is accepted but there are no subscribers.
    public static final byte REASON_CODE_NO_MATCHING_SUBSCRIBERS = 0x10;
    // 128 0x80 Unspecified error - The receiver does not accept the publish but either does not want to reveal the reason.
    public static final byte REASON_CODE_UNSPECIFIED_ERROR = (byte) 0x80;
    // 131 0x83 Implementation specific error - The PUBLISH is valid but the receiver is not willing to accept it.
    public static final byte REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR = (byte) 0x83;
    // 135 0x87 Not authorized - The PUBLISH is not authorized.
    public static final byte REASON_CODE_NOT_AUTHORIZED = (byte) 0x87;
    // 144 0x90 Topic Name invalid - The Topic Name is not malformed, but is not accepted by this Client or Server.
    public static final byte REASON_CODE_TOPIC_NAME_INVALID = (byte) 0x90;
    // 145 0x91 Packet identifier in use - The Packet Identifier is already in use.
    public static final byte REASON_CODE_PACKET_ID_IN_USE = (byte) 0x91;
    // 151 0x97 Quota exceeded - An implementation or administrative imposed limit has been exceeded.
    public static final byte REASON_CODE_QUOTA_EXCEEDED = (byte) 0x97;
    // 153 0x99 Payload format invalid - The payload format does not match the specified Payload Format Indicator.
    public static final byte REASON_CODE_PAYLOAD_FORMAT_INVALID = (byte) 0x99;
    // 146 0x92 The Packet Identifier is not known.
    // This is not an error during recovery, but at other times indicates a mismatch between the Session State on the Client and Server.
    public static final byte REASON_CODE_PACKET_ID_NOT_FOUND = (byte) 0x92;
    private static final ByteBufAllocator BYTE_BUF_ALLOCATOR = ByteBufAllocator.DEFAULT;
    protected final byte byte0;
    protected final int remainingLength;

    protected ControlPacket(byte byte0, int remainingLength) {
        this.byte0 = byte0;
        this.remainingLength = remainingLength;
    }

    /**
     * build incoming Packet
     *
     * @param incoming packet
     */
    protected ControlPacket(ByteBuf incoming) {
        this.byte0 = incoming.readByte();
        this.remainingLength = readRemainingLength(incoming);
    }

    /**
     * ByteBuf to model
     *
     * @param incoming the data packet which have just only one and whole packet data
     * @return model
     */
    public static ControlPacket from(ByteBuf incoming) {
        ControlPacket controlPacket = buildControlPacketFrom(incoming);
        // should read all the bytes out of the packet.
        if (incoming.isReadable()) {// control packet is illegal.
            throw new MalformedPacketException();
        }
        if (!controlPacket.packetValidate()) {
            log.error("ControlPacket validate failed -> {}", controlPacket);
            throw new MalformedPacketException();
        }
        return controlPacket;
    }

    /**
     * convert ByteBuf to ControlPacket
     *
     * @param incoming ByteBuf
     * @return ControlPacket
     */
    private static ControlPacket buildControlPacketFrom(ByteBuf incoming) {
        byte _0byte = incoming.getByte(incoming.readerIndex());
        return switch (type(_0byte)) {
            case CONNECT -> new Connect(incoming);
            case CONNACK -> new ConnAck(incoming);
            case PUBLISH ->/* core: zero-copy */ Publish.incoming(incoming);
            case PUBACK -> new PubAck(incoming);
            case PUBREC -> new PubRec(incoming);
            case PUBREL -> new PubRel(incoming);
            case PUBCOMP -> new PubComp(incoming);
            case SUBSCRIBE -> new Subscribe(incoming);
            case SUBACK -> new SubAck(incoming);
            case UNSUBSCRIBE -> new Unsubscribe(incoming);
            case UNSUBACK -> new UnsubAck(incoming);
            case PINGREQ -> new PingReq(incoming);
            case PINGRESP -> new PingResp(incoming);
            case DISCONNECT -> new Disconnect(incoming);
            default -> throw new IllegalArgumentException();
        };
    }

    public static int tryPickupPacket(ByteBuf in) {
        if (in.readableBytes() < MIN_PACKET_LENGTH) {
            return INCOMPLETE_PACKET;
        }
        in.markReaderIndex();
        try {
            in.readByte();
            int remainingLength = readRemainingLength(in);
            if (in.readableBytes() < remainingLength) {
                return INCOMPLETE_PACKET;
            }
            // fixed header length + remainingLength
            return _0_BYTE_LENGTH + variableByteIntegerLength(remainingLength) + remainingLength;
        } catch (Exception e) {
            log.error("tryPickupPacket failed: {}", ByteBufUtil.hexDump(in));
            throw e;
        } finally {
            in.resetReaderIndex();
        }
    }

    public static ControlPacket fromV50(ByteBuf incoming) {
        ControlPacket controlPacket = buildControlPacketFromV50(incoming);
        // should read all the bytes out of the packet.
        if (incoming.isReadable()) {// control packet is illegal.
            throw new MalformedPacketException();
        }
        if (!controlPacket.packetValidate()) {
            throw new MalformedPacketException();
        }
        return controlPacket;
    }

    private static ControlPacket buildControlPacketFromV50(ByteBuf incoming) {
        byte _0byte = incoming.getByte(incoming.readerIndex());
        return switch (type(_0byte)) {
            case CONNECT -> new Connect(incoming);
            case CONNACK -> new ConnAck(incoming);
            case PUBLISH ->/* core: zero-copy */ Publish.V50.incoming(incoming);
            case PUBACK -> new PubAck(incoming);
            case PUBREC -> new PubRec(incoming);
            case PUBREL -> new PubRel(incoming);
            case PUBCOMP -> new PubComp(incoming);
            case SUBSCRIBE -> new Subscribe(incoming);
            case SUBACK -> new SubAck(incoming);
            case UNSUBSCRIBE -> new Unsubscribe(incoming);
            case UNSUBACK -> new UnsubAck(incoming);
            case PINGREQ -> new PingReq(incoming);
            case PINGRESP -> new PingResp(incoming);
            case DISCONNECT -> new Disconnect(incoming);
            default -> throw new IllegalArgumentException();
        };
    }

    private static byte type(byte _0byte) {
        return (byte) (_0byte & 0xF0);
    }

    static int readRemainingLength(ByteBuf buf) {
        int rl = 0;
        int multiplier = 1;
        while (true) {
            if (buf.readableBytes() == 0) {
                // remainLength is 4 bytes, but now just received 2 bytes
                return Integer.MAX_VALUE;
            }
            byte encodeByte = buf.readByte();
            rl += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            multiplier *= 0x80;
            if (multiplier > 0x80 * 0x80 * 0x80) {
                throw new IllegalArgumentException();
            }
        }
        return rl;
    }

    protected static ByteBuf directBuffer(int capacity) {
        return BYTE_BUF_ALLOCATOR.directBuffer(capacity);
    }

    /**
     * heap buffer 强制使用 Unpooled // BUG: 若使用 Pooled，需要手动释放
     */
    protected static ByteBuf heapBuffer(int capacity) {
        return Unpooled.buffer(capacity);
    }

    protected static CompositeByteBuf compositeBuffer() {
        return BYTE_BUF_ALLOCATOR.compositeDirectBuffer();
    }

    static ByteBuf writeVariableByteInteger(ByteBuf buf, int remainingLength) {
        int rl = remainingLength;
        do {
            int encodedByte = rl % 128;
            rl /= 128;
            if (rl > 0) {
                encodedByte = (encodedByte | 128);
            }
            buf.writeByte(encodedByte);
        } while (rl > 0);
        return buf;
    }

    public static String hexPId(short packetIdentifier) {
        return "0x" + Integer.toHexString(packetIdentifier & 0xffff);
    }

    public static Short hexPIdToShort(String hexPId) {
        return Integer.valueOf(hexPId.substring(2), 16).shortValue();
    }

    /**
     * Bits in a byte are labelled 7 to 0. Bit number 7 is the most significant bit, the least significant bit is
     * assigned bit number 0.
     */
    public static byte readByte(ByteBuf buf) {
        return buf.readByte();
    }

    /**
     * Bits in a byte are labelled 7 to 0. Bit number 7 is the most significant bit, the least significant bit is
     * assigned bit number 0.
     */
    public static ByteBuf writeByte(ByteBuf buf, byte val) {
        return buf.writeByte(val);
    }

    /**
     * Two Byte Integer data values are 16-bit unsigned integers in big-endian order
     */
    public static int readTwoByteInteger(ByteBuf buf) {
        return buf.readUnsignedShort();
    }

    /**
     * Two Byte Integer data values are 16-bit unsigned integers in big-endian order
     */
    public static ByteBuf writeTwoByteInteger(ByteBuf buf, int val) {
        return buf.writeShort(val);
    }

    /**
     * Four Byte Integer data values are 32-bit unsigned integers in big-endian order
     */
    public static long readFourByteInteger(ByteBuf buf) {
        return buf.readUnsignedInt();
    }

    /**
     * Four Byte Integer data values are 32-bit unsigned integers in big-endian order
     */
    public static ByteBuf writeFourByteInteger(ByteBuf buf, long val) {
        return buf.writeInt((int) val);
    }

    /**
     * the maximum size of a UTF-8 Encoded String is 65,535 bytes
     */
    public static String readUTF8String(ByteBuf buf) {
        // todo: must use buf.readUnsignedShort() to decode the string length
        return buf.readCharSequence(buf.readUnsignedShort(), UTF_8).toString();
    }

    /**
     * all UTF-8 encoded strings can have any length in the range 0 to 65535 bytes
     */
    public static ByteBuf writeUTF8String(ByteBuf buf, String str) {
        if (str == null) {
            throw new IllegalArgumentException();
        }
        byte[] bytes = str.getBytes(UTF_8);
        // 明确表示使用 unsigned short 表示
        buf.writeShort((short) (bytes.length & 0xffff));
        buf.writeBytes(bytes);
        return buf;
    }

    /**
     * MAX: 268,435,455 (0xFF, 0xFF, 0xFF, 0x7F) < Integer.MAX 2,147,483,647
     */
    public static int readVariableByteInteger(ByteBuf buf) {
        int rl = 0;
        int multiplier = 1;
        while (true) {
            if (buf.readableBytes() == 0 /* not enough bytes */
                    || multiplier > 0x80 * 0x80 * 0x80 /* too many bytes */) {
                throw new IllegalArgumentException();
            }
            byte encodeByte = buf.readByte();
            rl += (encodeByte & 0x7F) * multiplier;
            if ((encodeByte & 0x80) == 0) {
                break;
            }
            multiplier *= 0x80;
        }
        return rl;
    }

    public static int variableByteIntegerLength(int value) {
        int ret = 0;
        do {
            ret += 1;
            value /= 128;
        } while (value > 0);
        return ret;
    }

    /**
     * Binary Data is represented by a Two Byte Integer length which indicates the number of data bytes, followed by
     * that number of bytes
     */
    public static ByteBuf readBinaryData(ByteBuf buf) {
        int length = buf.readUnsignedShort();
        ByteBuf ret = heapBuffer(length);
        buf.readBytes(ret);
        return ret;
    }

    /**
     * Binary Data is represented by a Two Byte Integer length which indicates the number of data bytes, followed by
     * that number of bytes
     */
    static ByteBuf writeBinaryData(ByteBuf buf, ByteBuf data) {
        if (data == null) {
            throw new IllegalArgumentException();
        }
        // 明确表示使用 unsigned short 表示
        buf.writeShort((short) (data.readableBytes() & 0xffff));
        buf.writeBytes(data);
        return buf;
    }

    public static boolean validateTopicName(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            return false;
        }
        return !topicName.contains(MULTI_LEVEL_WILDCARD)
                && !topicName.contains(SINGLE_LEVEL_WILDCARD);
    }

    public static ByteBuf writeProperties(ByteBuf buf, Properties properties) {
        properties.writeToByteBuf(buf);
        return buf;
    }

    public static boolean validateReasonCode(byte reasonCode) {
        return reasonCode == REASON_CODE_SUCCESS
                || reasonCode == REASON_CODE_NO_MATCHING_SUBSCRIBERS
                || reasonCode == REASON_CODE_UNSPECIFIED_ERROR
                || reasonCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
                || reasonCode == REASON_CODE_NOT_AUTHORIZED
                || reasonCode == REASON_CODE_TOPIC_NAME_INVALID
                || reasonCode == REASON_CODE_PACKET_ID_IN_USE
                || reasonCode == REASON_CODE_QUOTA_EXCEEDED
                || reasonCode == REASON_CODE_PAYLOAD_FORMAT_INVALID;

    }

    /**
     * validate the packet after build it
     */
    protected boolean packetValidate() {
        return true;
    }

    public byte type() {
        return type(this.byte0);
    }

    /**
     * model to ByteBuf
     *
     * @return ByteBuf
     */
    public ByteBuf toByteBuf() {
        // use direct buf will optimize netty zero-copy when write to Channel
        /** {@link Publish#toByteBuf()} */
        /** {@link AbstractNioByteChannel#filterOutboundMessage(Object)} */
        int packetLength = _0_BYTE_LENGTH + variableByteIntegerLength(remainingLength) + remainingLength;
        ByteBuf buf = directBuffer(packetLength);
        writeByte(buf, this.byte0);
        // remainingLength field
        writeVariableByteInteger(buf, remainingLength);
        return buf;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"byte0\":\"0x").append(String.format("%02X", byte0)).append("\",");
        sb.append("\"remainingLength\":").append(remainingLength).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    /**
     * 数据类型枚举
     */
    public enum DataRepresentation {
        BYTE,
        TWO_BYTE_INTEGER,
        FOUR_BYTE_INTEGER,
        VARIABLE_BYTE_INTEGER,
        UTF_8_ENCODED_STRING,
        BINARY_DATA,
        UTF_8_STRING_PAIR
    }

    public static class Properties {
        // MQTT 5.0 Properties 静态常量定义
        // 消息相关属性
        public static final int PAYLOAD_FORMAT_INDICATOR = 0x01;
        // If present, the Four Byte value is the lifetime of the Application Message in seconds
        public static final int MESSAGE_EXPIRY_INTERVAL = 0x02;
        public static final int CONTENT_TYPE = 0x03;

        // 响应相关属性
        public static final int RESPONSE_TOPIC = 0x08;
        public static final int CORRELATION_DATA = 0x09;

        // 订阅相关属性
        public static final int SUBSCRIPTION_IDENTIFIER = 0x0B;

        // 会话相关属性
        public static final int SESSION_EXPIRY_INTERVAL = 0x11;

        // 客户端标识符和服务器相关属性
        public static final int ASSIGNED_CLIENT_IDENTIFIER = 0x12;
        public static final int SERVER_KEEP_ALIVE = 0x13;

        // 认证相关属性
        public static final int AUTHENTICATION_METHOD = 0x15;
        public static final int AUTHENTICATION_DATA = 0x16;

        // 问题信息相关属性
        public static final int REQUEST_PROBLEM_INFORMATION = 0x17;
        public static final int WILL_DELAY_INTERVAL = 0x18;
        public static final int REQUEST_RESPONSE_INFORMATION = 0x19;
        public static final int RESPONSE_INFORMATION = 0x1A;
        public static final int SERVER_REFERENCE = 0x1C;

        // QoS 和消息限制相关属性
        public static final int RECEIVE_MAXIMUM = 0x21;
        public static final int TOPIC_ALIAS_MAXIMUM = 0x22;
        public static final int TOPIC_ALIAS = 0x23;
        public static final int MAXIMUM_QoS = 0x24;
        public static final int RETAIN_AVAILABLE = 0x25;

        // 包大小相关属性
        public static final int MAXIMUM_PACKET_SIZE = 0x27;

        // 功能可用性相关属性
        public static final int WILDCARD_SUBSCRIPTION_AVAILABLE = 0x28;
        public static final int SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29;
        public static final int SHARED_SUBSCRIPTION_AVAILABLE = 0x2A;

        // 问题和错误相关属性
        public static final int REASON_STRING = 0x1F;

        // 用户属性
        public static final int USER_PROPERTY = 0x26;
        public static final Properties EMPTY = new Properties();

        final List<Property> properties;

        Properties() {
            this(emptyList());
        }

        Properties(List<Property> properties) {
            if (properties == null) {
                throw new MalformedPacketException();
            }
            this.properties = properties;
        }

        public static Properties incoming(ByteBuf byteBuf) {
            return new Properties(decode(byteBuf));
        }

        private static List<Property> decode(ByteBuf buf) {
            List<Property> properties = new ArrayList<>();
            while (buf.isReadable()) {
                int id = readVariableByteInteger(buf);
                properties.add(decodeProperty(buf, id));
            }
            validateProperties(properties);
            return properties;
        }

        private static void validateProperties(List<Property> properties) {
            // 42 is the max id of MQTT 5.0
            byte[] ids = new byte[43];
            for (Property p : properties) {
                ids[p.id] += 1;
                if (ids[p.id] > 1 && p.id != USER_PROPERTY) {
                    throw new MalformedPacketException();
                }
            }
        }

        private static Property decodeProperty(ByteBuf buf, int id) {
            return switch (id) {
                case PAYLOAD_FORMAT_INDICATOR -> decodePayloadFormatIndicator(buf);
                case MESSAGE_EXPIRY_INTERVAL -> decodeMessageExpiryInterval(buf);
                case CONTENT_TYPE -> decodeContentType(buf);
                case RESPONSE_TOPIC -> decodeResponseTopic(buf);
                case CORRELATION_DATA -> decodeCorrelationData(buf);
                case SUBSCRIPTION_IDENTIFIER -> decodeSubscriptionIdentifier(buf);
                case SESSION_EXPIRY_INTERVAL -> decodeSessionExpiryInterval(buf);
                case ASSIGNED_CLIENT_IDENTIFIER -> decodeAssignedClientIdentifier(buf);
                case SERVER_KEEP_ALIVE -> decodeServerKeepAlive(buf);
                case AUTHENTICATION_METHOD -> decodeAuthenticationMethod(buf);
                case AUTHENTICATION_DATA -> decodeAuthenticationData(buf);
                case REQUEST_PROBLEM_INFORMATION -> decodeRequestProblemInformation(buf);
                case WILL_DELAY_INTERVAL -> decodeWillDelayInterval(buf);
                case REQUEST_RESPONSE_INFORMATION -> decodeRequestResponseInformation(buf);
                case RESPONSE_INFORMATION -> decodeResponseInformation(buf);
                case SERVER_REFERENCE -> decodeServerReference(buf);
                case RECEIVE_MAXIMUM -> decodeReceiveMaximum(buf);
                case TOPIC_ALIAS_MAXIMUM -> decodeTopicAliasMaximum(buf);
                case TOPIC_ALIAS -> decodeTopicAlias(buf);
                case MAXIMUM_QoS -> decodeMaximumQos(buf);
                case RETAIN_AVAILABLE -> decodeRetainAvailable(buf);
                case MAXIMUM_PACKET_SIZE -> decodeMaximumPacketSize(buf);
                case WILDCARD_SUBSCRIPTION_AVAILABLE -> decodeWildcardSubscriptionAvailable(buf);
                case SUBSCRIPTION_IDENTIFIER_AVAILABLE -> decodeSubscriptionIdentifierAvailable(buf);
                case SHARED_SUBSCRIPTION_AVAILABLE -> decodeSharedSubscriptionAvailable(buf);
                case REASON_STRING -> decodeReasonString(buf);
                case USER_PROPERTY -> decodeUserProperty(buf);
                default -> throw new IllegalArgumentException("unknown property id: " + id);
            };
        }

        private static ByteProperty decodePayloadFormatIndicator(ByteBuf buf) {
            byte val = readByte(buf);
            // 0 (0x00) Byte Indicates that the Payload is unspecified bytes, which is equivalent to not sending a Payload Format Indicator.
            // 1 (0x01) Byte Indicates that the Payload is UTF-8 Encoded Character Data.
            if (val == 0x00 || val == 0x01) {
                return new ByteProperty(PAYLOAD_FORMAT_INDICATOR, val);
            }
            throw new IllegalArgumentException("PAYLOAD_FORMAT_INDICATOR must be 0 or 1, got: " + val);
        }

        private static FourByteIntegerProperty decodeMessageExpiryInterval(ByteBuf buf) {
            return new FourByteIntegerProperty(MESSAGE_EXPIRY_INTERVAL, readFourByteInteger(buf));
        }

        private static UTF8EncodedStringProperty decodeContentType(ByteBuf buf) {
            return new UTF8EncodedStringProperty(CONTENT_TYPE, readUTF8String(buf));
        }

        private static UTF8EncodedStringProperty decodeResponseTopic(ByteBuf buf) {
            String val = readUTF8String(buf);
            if (!validateResponseTopicName(val)) {
                throw new IllegalArgumentException("Invalid response topic format: " + val);
            }
            return new UTF8EncodedStringProperty(RESPONSE_TOPIC, val);
        }

        private static BinaryDataProperty decodeCorrelationData(ByteBuf buf) {
            return new BinaryDataProperty(CORRELATION_DATA, readBinaryData(buf));
        }

        private static VariableByteIntegerProperty decodeSubscriptionIdentifier(ByteBuf buf) {
            int val = readVariableByteInteger(buf);
            if (val <= 0) {
                throw new IllegalArgumentException("SUBSCRIPTION_IDENTIFIER must be greater than 0, got: " + val);
            }
            return new VariableByteIntegerProperty(SUBSCRIPTION_IDENTIFIER, val);
        }

        private static FourByteIntegerProperty decodeSessionExpiryInterval(ByteBuf buf) {
            return new FourByteIntegerProperty(SESSION_EXPIRY_INTERVAL, readFourByteInteger(buf));
        }

        private static UTF8EncodedStringProperty decodeAssignedClientIdentifier(ByteBuf buf) {
            return new UTF8EncodedStringProperty(ASSIGNED_CLIENT_IDENTIFIER, readUTF8String(buf));
        }

        private static TwoByteIntegerProperty decodeServerKeepAlive(ByteBuf buf) {
            return new TwoByteIntegerProperty(SERVER_KEEP_ALIVE, readTwoByteInteger(buf));
        }

        private static UTF8EncodedStringProperty decodeAuthenticationMethod(ByteBuf buf) {
            return new UTF8EncodedStringProperty(AUTHENTICATION_METHOD, readUTF8String(buf));
        }

        private static BinaryDataProperty decodeAuthenticationData(ByteBuf buf) {
            return new BinaryDataProperty(AUTHENTICATION_DATA, readBinaryData(buf));
        }

        private static ByteProperty decodeRequestProblemInformation(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("REQUEST_PROBLEM_INFORMATION must be 0 or 1, got: " + val);
            }
            return new ByteProperty(REQUEST_PROBLEM_INFORMATION, val);
        }

        private static FourByteIntegerProperty decodeWillDelayInterval(ByteBuf buf) {
            return new FourByteIntegerProperty(WILL_DELAY_INTERVAL, readFourByteInteger(buf));
        }

        private static ByteProperty decodeRequestResponseInformation(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("REQUEST_RESPONSE_INFORMATION must be 0 or 1, got: " + val);
            }
            return new ByteProperty(REQUEST_RESPONSE_INFORMATION, val);
        }

        private static UTF8EncodedStringProperty decodeResponseInformation(ByteBuf buf) {
            return new UTF8EncodedStringProperty(RESPONSE_INFORMATION, readUTF8String(buf));
        }

        private static UTF8EncodedStringProperty decodeServerReference(ByteBuf buf) {
            return new UTF8EncodedStringProperty(SERVER_REFERENCE, readUTF8String(buf));
        }

        private static TwoByteIntegerProperty decodeReceiveMaximum(ByteBuf buf) {
            int val = readTwoByteInteger(buf);
            if (val == 0) {
                throw new IllegalArgumentException("RECEIVE_MAXIMUM must be greater than 0, got: " + val);
            }
            return new TwoByteIntegerProperty(RECEIVE_MAXIMUM, val);
        }

        private static TwoByteIntegerProperty decodeTopicAliasMaximum(ByteBuf buf) {
            return new TwoByteIntegerProperty(TOPIC_ALIAS_MAXIMUM, readTwoByteInteger(buf));
        }

        private static TwoByteIntegerProperty decodeTopicAlias(ByteBuf buf) {
            int val = readTwoByteInteger(buf);
            if (val <= 0) {
                throw new IllegalArgumentException("TOPIC_ALIAS must be greater than 0, got: " + val);
            }
            return new TwoByteIntegerProperty(TOPIC_ALIAS, val);
        }

        private static ByteProperty decodeMaximumQos(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("MAXIMUM_QoS must be 0 or 1, got: " + val);
            }
            return new ByteProperty(MAXIMUM_QoS, val);
        }

        private static ByteProperty decodeRetainAvailable(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("RETAIN_AVAILABLE must be 0 or 1, got: " + val);
            }
            return new ByteProperty(RETAIN_AVAILABLE, val);
        }

        private static FourByteIntegerProperty decodeMaximumPacketSize(ByteBuf buf) {
            long val = readFourByteInteger(buf);
            if (val == 0) {
                // It is a Protocol Error to include the Maximum Packet Size more than once, or for the value to be set to zero.
                throw new IllegalArgumentException("MAXIMUM_PACKET_SIZE cannot be zero");
            }
            return new FourByteIntegerProperty(MAXIMUM_PACKET_SIZE, val);
        }

        private static ByteProperty decodeWildcardSubscriptionAvailable(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("WILDCARD_SUBSCRIPTION_AVAILABLE must be 0 or 1, got: " + val);
            }
            return new ByteProperty(WILDCARD_SUBSCRIPTION_AVAILABLE, val);
        }

        private static ByteProperty decodeSubscriptionIdentifierAvailable(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("SUBSCRIPTION_IDENTIFIER_AVAILABLE must be 0 or 1, got: " + val);
            }
            return new ByteProperty(SUBSCRIPTION_IDENTIFIER_AVAILABLE, val);
        }

        private static ByteProperty decodeSharedSubscriptionAvailable(ByteBuf buf) {
            byte val = readByte(buf);
            if (val != 0x00 && val != 0x01) {
                throw new IllegalArgumentException("SHARED_SUBSCRIPTION_AVAILABLE must be 0 or 1, got: " + val);
            }
            return new ByteProperty(SHARED_SUBSCRIPTION_AVAILABLE, val);
        }

        private static UTF8EncodedStringProperty decodeReasonString(ByteBuf buf) {
            return new UTF8EncodedStringProperty(REASON_STRING, readUTF8String(buf));
        }

        private static UTF8StringPairProperty decodeUserProperty(ByteBuf buf) {
            return new UTF8StringPairProperty(USER_PROPERTY, readUTF8String(buf), readUTF8String(buf));
        }

        private static boolean validateResponseTopicName(String topic) {
            if (!validateTopicName(topic)) {
                return false;
            }
            // Response Topic MUST NOT contain wildcard characters
            return !topic.contains("+") && !topic.contains("#");
        }

        public boolean isEmpty() {
            return properties.isEmpty();
        }

        public ByteBuf writeToByteBuf(ByteBuf buf) {
            writeVariableByteInteger(buf, calcPropertyLength());
            for (Property p : properties) {
                p.write(buf);
            }
            return buf;
        }

        public int calcPropertyLength() {
            int propertyLength = 0;
            for (Property p : properties) {
                propertyLength += p.bytesLength();
            }
            return propertyLength;
        }

    }

    static abstract class Property {
        final int id;

        public Property(int id) {
            this.id = id;
        }

        abstract int bytesLength();

        abstract ByteBuf write(ByteBuf buf);
    }

    static class ByteProperty extends Property {
        final byte value;

        public ByteProperty(int type, byte value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 2;
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeByte(buf, value);
            return buf;
        }
    }

    static class TwoByteIntegerProperty extends Property {
        final int value;

        public TwoByteIntegerProperty(int type, int value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 3;
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeTwoByteInteger(buf, value);
            return buf;
        }
    }

    static class FourByteIntegerProperty extends Property {
        final long value;

        public FourByteIntegerProperty(int type, long value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 5;
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeFourByteInteger(buf, value);
            return buf;
        }
    }

    static class UTF8EncodedStringProperty extends Property {
        final String value;

        public UTF8EncodedStringProperty(int type, String value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 1 + 2 + value.getBytes(UTF_8).length;
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeUTF8String(buf, value);
            return buf;
        }
    }

    static class BinaryDataProperty extends Property {
        final ByteBuf value;

        public BinaryDataProperty(int type, ByteBuf value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 1 + 2 + value.readableBytes();
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeBinaryData(buf, value);
            return buf;
        }
    }

    static class VariableByteIntegerProperty extends Property {
        final int value;

        public VariableByteIntegerProperty(int type, int value) {
            super(type);
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 1 + variableByteIntegerLength(value);
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeVariableByteInteger(buf, value);
            return buf;
        }
    }

    static class UTF8StringPairProperty extends Property {
        final String key;
        final String value;

        public UTF8StringPairProperty(int type, String key, String value) {
            super(type);
            this.key = key;
            this.value = value;
        }

        @Override
        int bytesLength() {
            return 1 + 2 + key.getBytes(UTF_8).length + 2 + value.getBytes(UTF_8).length;
        }

        @Override
        ByteBuf write(ByteBuf buf) {
            writeVariableByteInteger(buf, id);
            writeUTF8String(buf, key);
            writeUTF8String(buf, value);
            return buf;
        }
    }

    /**
     * 抛出此异常表示数据包格式错误
     */
    public static class MalformedPacketException extends RuntimeException {

        public MalformedPacketException() {
        }

        public MalformedPacketException(String msg) {
            super(msg);
        }
    }

    /**
     * <pre>
     *     If the Server included a Maximum QoS in its CONNACK response to a Client and it receives a PUBLISH
     * packet with a QoS greater than this, then it uses DISCONNECT with Reason Code 0x9B (QoS not
     * supported) as described in section 4.13 Handling errors.
     * </pre>
     */
    public static class QoSNotSupportedException extends RuntimeException {

        public QoSNotSupportedException() {
        }

        public QoSNotSupportedException(String msg) {
            super(msg);
        }
    }

    /**
     * <pre>
     *     If the Server included Retain Available in its CONNACK response to a Client with its value set to 0 and it
     * receives a PUBLISH packet with the RETAIN flag is set to 1, then it uses the DISCONNECT Reason
     * Code of 0x9A (Retain not supported) as described in section 4.13.
     * </pre>
     */
    public static class RetainNotSupportedException extends RuntimeException {

        public RetainNotSupportedException() {
        }

        public RetainNotSupportedException(String msg) {
            super(msg);
        }
    }

}