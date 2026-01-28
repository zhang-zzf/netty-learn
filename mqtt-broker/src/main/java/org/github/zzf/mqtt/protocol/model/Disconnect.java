package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_ADMINISTRATIVE_ACTION;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_CONNECTION_RATE_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_DISCONNECT_WITH_WILL_MSG;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_KEEP_ALIVE_TIMEOUT;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_MALFORMED_PACKET;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_MAXIMUM_CONNECT_TIME;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_MESSAGE_RATE_TOO_HIGH;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_NOT_AUTHORIZED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PACKET_TOO_LARGE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PAYLOAD_FORMAT_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PROTOCOL_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_QOS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_QUOTA_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_RETAIN_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_BUSY;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_MOVED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SERVER_SHUTTING_DOWN;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SESSION_TAKEN_OVER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SUCCESS;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_TOPIC_ALIAS_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_TOPIC_FILTER_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_TOPIC_NAME_INVALID;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_UNSPECIFIED_ERROR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_USE_ANOTHER_SERVER;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SERVER_REFERENCE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SESSION_EXPIRY_INTERVAL;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.Set;

public class Disconnect extends ControlPacket {

    public static final byte _0_BYTE = (byte) 0xE0;

    private Disconnect(byte byte0, int remainingLength) {
        super(byte0, remainingLength);
    }

    static Disconnect incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        return new Disconnect(byte0, remainingLength);
    }

    public static Disconnect from() {
        return new Disconnect(_0_BYTE, 0x00);
    }

    @Override
    public boolean packetValidate() {
        return this.byte0 == _0_BYTE;
    }

    public static class V50 extends Disconnect {

        final byte reasonCode;
        final Properties properties;
        final Set<Integer> allowedProperties = Set.of(
                SESSION_EXPIRY_INTERVAL,
                REASON_STRING,
                USER_PROPERTY,
                SERVER_REFERENCE
        );

        V50(byte byte0, int remainingLength,
                byte reasonCode, Properties properties) {
            super(byte0, remainingLength);
            this.reasonCode = reasonCode;
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            byte reasonCode = readByte(incoming);
            Properties properties = readProperties(incoming);
            return new V50(byte0, remainingLength, reasonCode, properties);
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && validateReasonCode()
                    && properties.validateIdentifier(allowedProperties);
        }

        boolean validateReasonCode() {
            return reasonCode == REASON_CODE_SUCCESS  // 0x00 Normal disconnection
                    || reasonCode == REASON_CODE_DISCONNECT_WITH_WILL_MSG  // 0x04 Disconnect with Will Message
                    || reasonCode == REASON_CODE_UNSPECIFIED_ERROR  // 0x80 Unspecified error
                    || reasonCode == REASON_CODE_MALFORMED_PACKET  // 0x81 Malformed Packet
                    || reasonCode == REASON_CODE_PROTOCOL_ERROR  // 0x82 Protocol Error
                    || reasonCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR  // 0x83 Implementation specific error
                    || reasonCode == REASON_CODE_NOT_AUTHORIZED  // 0x87 Not authorized
                    || reasonCode == REASON_CODE_SERVER_BUSY  // 0x89 Server busy
                    || reasonCode == REASON_CODE_SERVER_SHUTTING_DOWN  // 0x8B Server shutting down
                    || reasonCode == REASON_CODE_KEEP_ALIVE_TIMEOUT  // 0x8D Keep Alive timeout
                    || reasonCode == REASON_CODE_SESSION_TAKEN_OVER  // 0x8E Session taken over
                    || reasonCode == REASON_CODE_TOPIC_FILTER_INVALID  // 0x8F Topic Filter invalid
                    || reasonCode == REASON_CODE_TOPIC_NAME_INVALID  // 0x90 Topic Name invalid
                    || reasonCode == REASON_CODE_RECEIVE_MAXIMUM_EXCEEDED  // 0x93 Receive Maximum exceeded
                    || reasonCode == REASON_CODE_TOPIC_ALIAS_INVALID  // 0x94 Topic Alias invalid
                    || reasonCode == REASON_CODE_PACKET_TOO_LARGE  // 0x95 Packet too large
                    || reasonCode == REASON_CODE_MESSAGE_RATE_TOO_HIGH  // 0x96 Message rate too high
                    || reasonCode == REASON_CODE_QUOTA_EXCEEDED  // 0x97 Quota exceeded
                    || reasonCode == REASON_CODE_ADMINISTRATIVE_ACTION  // 0x98 Administrative action
                    || reasonCode == REASON_CODE_PAYLOAD_FORMAT_INVALID  // 0x99 Payload format invalid
                    || reasonCode == REASON_CODE_RETAIN_NOT_SUPPORTED  // 0x9A Retain not supported
                    || reasonCode == REASON_CODE_QOS_NOT_SUPPORTED  // 0x9B QoS not supported
                    || reasonCode == REASON_CODE_USE_ANOTHER_SERVER  // 0x9C Use another server
                    || reasonCode == REASON_CODE_SERVER_MOVED  // 0x9D Server moved
                    || reasonCode == REASON_CODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
                    // 0x9E Shared Subscriptions not supported
                    || reasonCode == REASON_CODE_CONNECTION_RATE_EXCEEDED  // 0x9F Connection rate exceeded
                    || reasonCode == REASON_CODE_MAXIMUM_CONNECT_TIME  // 0xA0 Maximum connect time
                    || reasonCode == REASON_CODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
                    // 0xA1 Subscription Identifiers not supported
                    || reasonCode
                    == REASON_CODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;  // 0xA2 Wildcard Subscriptions not supported
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toByteBuf();
            writeByte(buf, reasonCode);
            writeProperties(buf, properties);
            return buf;
        }
    }

}
