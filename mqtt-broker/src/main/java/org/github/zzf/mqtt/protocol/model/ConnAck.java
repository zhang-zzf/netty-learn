package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_BAD_AUTHENTICATION_METHOD;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_BAD_USER_NAME_OR_PASSWORD;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_BANNED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_CLIENT_IDENTIFIER_NOT_VALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_CONNECTION_RATE_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_MALFORMED_PACKET;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_NOT_AUTHORIZED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PACKET_TOO_LARGE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PAYLOAD_FORMAT_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PROTOCOL_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_QOS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_QUOTA_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_RETAIN_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_BUSY;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_MOVED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_UNAVAILABLE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SUCCESS;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_TOPIC_NAME_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_UNSPECIFIED_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_USE_ANOTHER_SERVER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.ASSIGNED_CLIENT_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.AUTHENTICATION_DATA;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.AUTHENTICATION_METHOD;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.EMPTY;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.MAXIMUM_PACKET_SIZE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.MAXIMUM_QoS;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RECEIVE_MAXIMUM;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RESPONSE_INFORMATION;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RETAIN_AVAILABLE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SERVER_KEEP_ALIVE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SERVER_REFERENCE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SESSION_EXPIRY_INTERVAL;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SHARED_SUBSCRIPTION_AVAILABLE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SUBSCRIPTION_IDENTIFIER_AVAILABLE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.TOPIC_ALIAS_MAXIMUM;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.TOPIC_ALIAS_MAXIMUM_DEFAULT_VALUE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.WILDCARD_SUBSCRIPTION_AVAILABLE;

import io.netty.buffer.ByteBuf;
import java.util.Set;

public class ConnAck extends ControlPacket {

    public static final byte ACCEPTED = 0x00;
    public static final byte UNACCEPTED_PROTOCOL_VERSION = 0x01;
    public static final byte IDENTIFIER_REJECTED = 0x02;
    public static final byte SERVER_UNAVAILABLE = 0x03;
    public static final byte BAD_USER_NAME_OR_PASSWORD = 0x04;
    public static final byte NOT_AUTHORIZED = 0x05;
    public static final byte BYTE_0 = (byte) 0x20;

    final byte connectAcknowledgeFlags;
    final byte returnCode;

    private ConnAck(byte byte0, int remainingLength,
            byte connectAcknowledgeFlags, byte returnCode) {
        super(byte0, remainingLength);
        this.connectAcknowledgeFlags = connectAcknowledgeFlags;
        this.returnCode = returnCode;
    }

    public static ConnAck incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        byte connectAcknowledgeFlags = readByte(incoming);
        byte returnCode = readByte(incoming);
        return new ConnAck(byte0, remainingLength,
                connectAcknowledgeFlags, returnCode);
    }

    public static ConnAck authenticateFailed(byte returnCode) {
        if (returnCode == ACCEPTED) {
            throw new IllegalArgumentException();
        }
        return ConnAck.from(false, returnCode);
    }

    private static ConnAck from(byte byte0, int remainingLength,
            boolean sp, byte returnCode) {
        // If a server sends a CONNACK packet containing a non-zero return code
        // it MUST set Session Present to 0
        if (returnCode != 0 && sp) {
            throw new IllegalArgumentException();
        }
        byte connectAcknowledgeFlags = sp ? (byte) 0x01 : 0x00;
        ConnAck ret = new ConnAck(byte0, remainingLength,
                connectAcknowledgeFlags, returnCode);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    private static ConnAck from(boolean sp, byte returnCode) {
        return from(BYTE_0, 0x02, sp, returnCode);
    }


    public static ConnAck accepted() {
        return ConnAck.from(false, ACCEPTED);
    }

    /**
     * 0x01 Connection Refused, unacceptable protocol version
     */
    public static ConnAck notSupportProtocolLevel() {
        return ConnAck.from(false, UNACCEPTED_PROTOCOL_VERSION);
    }

    public static ConnAck acceptedWithStoredSession() {
        return ConnAck.from(true, ACCEPTED);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeByte(buf, connectAcknowledgeFlags);
        writeByte(buf, returnCode);
        return buf;
    }

    @Override
    protected boolean packetValidate() {
        return super.packetValidate()
                && validateConnectAcknowledgeFlags()
                && validateReturnCode()
                ;
    }

    boolean validateReturnCode() {
        return returnCode == ACCEPTED
                || returnCode == UNACCEPTED_PROTOCOL_VERSION
                || returnCode == IDENTIFIER_REJECTED
                || returnCode == SERVER_UNAVAILABLE
                || returnCode == BAD_USER_NAME_OR_PASSWORD
                || returnCode == NOT_AUTHORIZED;
    }

    private boolean validateConnectAcknowledgeFlags() {
        // Bits 7-1 are reserved and MUST be set to 0.
        return (connectAcknowledgeFlags & 0xFE) == 0x00;
    }

    ByteBuf toPacketBuf() {
        return super.toByteBuf();
    }

    public boolean sp() {
        return (connectAcknowledgeFlags & 0x01) != 0x00;
    }

    public int returnCode() {
        return this.returnCode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"sp\":").append(sp()).append(',');
        sb.append("\"returnCode\":").append(returnCode).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static class V50 extends ConnAck {
        public static final int ACKNOWLEDGE_FLAGS_LENGTH = 1;
        public static final int REASON_CODE_LENGTH = 1;
        final Properties properties;

        private V50(byte byte0, int remainingLength,
                byte connectAcknowledgeFlags, byte returnCode, Properties properties) {
            super(byte0, remainingLength, connectAcknowledgeFlags, returnCode);
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            byte connectAcknowledgeFlags = readByte(incoming);
            byte returnCode = readByte(incoming);
            Properties properties = readProperties(incoming);
            return new V50(byte0, remainingLength,
                    connectAcknowledgeFlags, returnCode, properties);
        }

        public static V50 accepted() {
            return V50.from(false, ACCEPTED, EMPTY);
        }

        public static V50 acceptedWithStoredSession() {
            return V50.from(true, ACCEPTED, EMPTY);
        }

        public static V50 authenticateFailed(byte authenticate) {
            return V50.from(true, NOT_AUTHORIZED, Properties.empty());
        }

        public static V50 notSupportProtocolLevel() {
            return V50.from(false, REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION, Properties.empty());
        }

        public static V50 from(boolean sp, byte returnCode, Properties properties) {
            int remainingLength = ACKNOWLEDGE_FLAGS_LENGTH
                    + REASON_CODE_LENGTH
                    + calcPropertiesLength(properties);
            byte connectAcknowledgeFlags = sp ? (byte) 0x01 : 0x00;
            V50 ret = new V50(BYTE_0, remainingLength,
                    connectAcknowledgeFlags, returnCode, properties);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return ret;
        }

        public int topicAliasMaximum() {
            return properties.topicAliasMaximum().orElse(TOPIC_ALIAS_MAXIMUM_DEFAULT_VALUE);
        }

        public Properties properties() {
            return properties;
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = toPacketBuf();
            writeByte(buf, connectAcknowledgeFlags);
            writeByte(buf, returnCode);
            writeProperties(buf, properties);
            return buf;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties);
        }

        @Override
        boolean validateReturnCode() {
            return returnCode == REASON_CODE_SUCCESS
                    || returnCode == REASON_CODE_UNSPECIFIED_ERROR
                    || returnCode == REASON_CODE_MALFORMED_PACKET
                    || returnCode == REASON_CODE_PROTOCOL_ERROR
                    || returnCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
                    || returnCode == REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION
                    || returnCode == REASON_CODE_CLIENT_IDENTIFIER_NOT_VALID
                    || returnCode == REASON_CODE_BAD_USER_NAME_OR_PASSWORD
                    || returnCode == REASON_CODE_NOT_AUTHORIZED
                    || returnCode == REASON_CODE_SERVER_UNAVAILABLE
                    || returnCode == REASON_CODE_SERVER_BUSY
                    || returnCode == REASON_CODE_BANNED
                    || returnCode == REASON_CODE_BAD_AUTHENTICATION_METHOD
                    || returnCode == REASON_CODE_TOPIC_NAME_INVALID
                    || returnCode == REASON_CODE_PACKET_TOO_LARGE
                    || returnCode == REASON_CODE_QUOTA_EXCEEDED
                    || returnCode == REASON_CODE_PAYLOAD_FORMAT_INVALID
                    || returnCode == REASON_CODE_RETAIN_NOT_SUPPORTED
                    || returnCode == REASON_CODE_QOS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_USE_ANOTHER_SERVER
                    || returnCode == REASON_CODE_SERVER_MOVED
                    || returnCode == REASON_CODE_CONNECTION_RATE_EXCEEDED
                    ;
        }

        final Set<Integer> allowedProperties = Set.of(
                SESSION_EXPIRY_INTERVAL,
                RECEIVE_MAXIMUM,
                MAXIMUM_QoS,
                RETAIN_AVAILABLE,
                MAXIMUM_PACKET_SIZE,
                ASSIGNED_CLIENT_IDENTIFIER,
                TOPIC_ALIAS_MAXIMUM,
                REASON_STRING,
                USER_PROPERTY,
                WILDCARD_SUBSCRIPTION_AVAILABLE,
                SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                SHARED_SUBSCRIPTION_AVAILABLE,
                SERVER_KEEP_ALIVE,
                RESPONSE_INFORMATION,
                SERVER_REFERENCE,
                AUTHENTICATION_METHOD,
                AUTHENTICATION_DATA
        );

    }


}
