package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.List;

public class SubAck extends ControlPacket {

    final short packetIdentifier;
    final byte[] returnCodes;

    public static SubAck incoming(ByteBuf incoming) {
        readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        byte[] returnCodes = readBytes(incoming, incoming.readableBytes());
        return new SubAck(remainingLength, packetIdentifier, returnCodes);
    }

    public static SubAck from(short packetIdentifier,
            List<Subscribe.Subscription> subscriptions) {
        int rl = 2 + subscriptions.size();
        byte[] returnCodes = new byte[subscriptions.size()];
        for (int i = 0; i < subscriptions.size(); i++) {
            returnCodes[i] = subscriptions.get(i).options;
        }
        SubAck ret = new SubAck(rl, packetIdentifier, returnCodes);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    public static SubAck from(List<Subscribe.Subscription> subscriptions) {
        return from((short) 0, subscriptions);
    }

    private SubAck(int remainingLength,
            short packetIdentifier,
            byte[] returnCodes) {
        super(SUBACK, remainingLength);
        this.packetIdentifier = packetIdentifier;
        this.returnCodes = returnCodes;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        writeBytes(buf, returnCodes);
        return buf;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(hexPId(packetIdentifier)).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static class V50 extends SubAck {

        // 0 0x00 Granted QoS 0 - The subscription is accepted and the maximum QoS sent will be QoS 0
        public static final byte REASON_CODE_GRANTED_QOS_0 = 0x00;
        // 1 0x01 Granted QoS 1 - The subscription is accepted and the maximum QoS sent will be QoS 1
        public static final byte REASON_CODE_GRANTED_QOS_1 = 0x01;
        // 2 0x02 Granted QoS 2 - The subscription is accepted and any received QoS will be sent to this subscription
        public static final byte REASON_CODE_GRANTED_QOS_2 = 0x02;
        // 128 0x80 Unspecified error - The subscription is not accepted and the Server either does not wish to reveal the reason
        public static final byte REASON_CODE_UNSPECIFIED_ERROR = (byte) 0x80;
        // 131 0x83 Implementation specific error - The SUBSCRIBE is valid but the Server does not accept it
        public static final byte REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR = (byte) 0x83;
        // 135 0x87 Not authorized - The Client is not authorized to make this subscription
        public static final byte REASON_CODE_NOT_AUTHORIZED = (byte) 0x87;
        // 143 0x8F Topic Filter invalid - The Topic Filter is correctly formed but is not allowed for this Client
        public static final byte REASON_CODE_TOPIC_FILTER_INVALID = (byte) 0x8F;
        // 145 0x91 Packet Identifier in use - The specified Packet Identifier is already in use
        public static final byte REASON_CODE_PACKET_ID_IN_USE = (byte) 0x91;
        // 151 0x97 Quota exceeded - An implementation or administrative imposed limit has been exceeded
        public static final byte REASON_CODE_QUOTA_EXCEEDED = (byte) 0x97;
        // 158 0x9E Shared Subscriptions not supported - The Server does not support Shared Subscriptions for this Client
        public static final byte REASON_CODE_SHARED_SUBS_NOT_SUPPORTED = (byte) 0x9E;
        // 161 0xA1 Subscription Identifiers not supported - The Server does not support Subscription Identifiers
        public static final byte REASON_CODE_SUBSCRIPTION_IDS_NOT_SUPPORTED = (byte) 0xA1;
        // 162 0xA2 Wildcard Subscriptions not supported - The Server does not support Wildcard Subscriptions
        public static final byte REASON_CODE_WILDCARD_SUBS_NOT_SUPPORTED = (byte) 0xA2;


        // If there are no properties, this MUST be indicated by including a Property Length of zero
        final Properties properties;

        V50(int remainingLength,
                short packetIdentifier, Properties properties,
                byte[] reasonCodes) {
            super(remainingLength, packetIdentifier, reasonCodes);
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            Properties properties = readProperties(incoming);
            byte[] reasonCodes = readBytes(incoming, incoming.readableBytes());
            return new V50(remainingLength,
                    packetIdentifier, properties,
                    reasonCodes);
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toPacketByteBuf();
            writeTwoByteInteger(buf, packetIdentifier);
            writeProperties(buf, properties);
            writeBytes(buf, returnCodes);
            return buf;
        }

        public Properties properties() {
            return this.properties;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && validateProperties()
                    && validateReasonCodes();
        }

        private boolean validateReasonCodes() {
            if (returnCodes.length == 0) {
                return false;
            }
            for (byte returnCode : returnCodes) {
                if (!isValidReasonCode(returnCode)) {
                    return false;
                }
            }
            return true;
        }

        private boolean isValidReasonCode(byte returnCode) {
            return returnCode == REASON_CODE_GRANTED_QOS_0
                    || returnCode == REASON_CODE_GRANTED_QOS_1
                    || returnCode == REASON_CODE_GRANTED_QOS_2
                    || returnCode == REASON_CODE_UNSPECIFIED_ERROR
                    || returnCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
                    || returnCode == REASON_CODE_NOT_AUTHORIZED
                    || returnCode == REASON_CODE_PACKET_ID_IN_USE
                    || returnCode == REASON_CODE_QUOTA_EXCEEDED
                    || returnCode == REASON_CODE_SHARED_SUBS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_SUBSCRIPTION_IDS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_WILDCARD_SUBS_NOT_SUPPORTED
                    || returnCode == REASON_CODE_TOPIC_FILTER_INVALID
                    ;
        }

        private boolean validateProperties() {
            for (Property p : this.properties.properties) {
                switch (p.id) {
                    case REASON_STRING:
                    case USER_PROPERTY:
                        break;
                    default:
                        return false;
                }
            }
            return true;
        }
    }

    private ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
    }
}
