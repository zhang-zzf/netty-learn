package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;

public class UnsubAck extends ControlPacket {

    final short packetIdentifier;

    UnsubAck(short packetIdentifier) {
        super((byte) 0xB0, 0x02);
        this.packetIdentifier = packetIdentifier;
    }

    public static UnsubAck incoming(ByteBuf incoming) {
        readByte(incoming);
        readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        return new UnsubAck(packetIdentifier);
    }

    public static UnsubAck from(short packetIdentifier) {
        return new UnsubAck(packetIdentifier);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        return buf;
    }

    private ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(hexPId(packetIdentifier)).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    public static class V50 extends UnsubAck {

        public static final byte REASON_CODE_SUCCESS = 0x00;
        public static final byte REASON_CODE_NO_SUBSCRIPTION_EXISTED = 0x11;
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

        final Properties properties;
        final byte[] reasonCodes;

        V50(byte byte0, int remainingLength,
                short packetIdentifier, Properties properties,
                byte[] reasonCodes) {
            super(packetIdentifier);
            this.properties = properties;
            this.reasonCodes = reasonCodes;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            Properties properties = readProperties(incoming);
            byte[] reasonCodes = new byte[incoming.readableBytes()];
            for (int i = 0; i < reasonCodes.length; i++) {
                reasonCodes[i] = readByte(incoming);
            }
            return new V50(byte0, remainingLength,
                    packetIdentifier, properties,
                    reasonCodes);
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toPacketByteBuf();
            writeTwoByteInteger(buf, packetIdentifier);
            writeProperties(buf, properties);
            writeBytes(buf, reasonCodes);
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
            if (reasonCodes == null || reasonCodes.length == 0) {
                return false;
            }
            for (byte reasonCode : reasonCodes) {
                if (!isValidReasonCode(reasonCode)) {
                    return false;
                }
            }
            return true;
        }

        private boolean isValidReasonCode(byte reasonCode) {
            return reasonCode == REASON_CODE_SUCCESS
                    || reasonCode == REASON_CODE_NO_SUBSCRIPTION_EXISTED
                    || reasonCode == REASON_CODE_UNSPECIFIED_ERROR
                    || reasonCode == REASON_CODE_IMPLEMENTATION_SPECIFIC_ERROR
                    || reasonCode == REASON_CODE_NOT_AUTHORIZED
                    || reasonCode == REASON_CODE_TOPIC_FILTER_INVALID
                    || reasonCode == REASON_CODE_PACKET_ID_IN_USE
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


}

