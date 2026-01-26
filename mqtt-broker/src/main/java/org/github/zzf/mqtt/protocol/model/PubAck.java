package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.*;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;

public class PubAck extends ControlPacket {

    public static final byte BYTE_0 = (byte) 0x40;
    final short packetIdentifier;

    private PubAck(byte byte0, int remainingLength,
            short packetIdentifier) {
        super(byte0, remainingLength);
        this.packetIdentifier = packetIdentifier;
    }

    public static PubAck incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        return new PubAck(byte0, remainingLength, packetIdentifier);
    }

    public static PubAck from(short packetIdentifier) {
        return new PubAck(BYTE_0, 0x02, packetIdentifier);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        return buf;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":").append(pId()).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public short packetIdentifier() {
        return packetIdentifier;
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }


    public static class V50 extends PubAck {

        final byte reasonCode;
        final Properties properties;

        private V50(byte byte0, int remainingLength,
                short packetIdentifier, byte reasonCode, Properties properties) {
            super(byte0, remainingLength, packetIdentifier);
            this.reasonCode = reasonCode;
            this.properties = properties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            byte reasonCode ;
            Properties properties ;
            if (incoming.isReadable()) {
                reasonCode = readByte(incoming);
                properties = readProperties(incoming);
            }
            else {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
                // and there are no Properties. In this case the PUBACK has a Remaining Length of 2.
                reasonCode = REASON_CODE_SUCCESS;
                properties = Properties.EMPTY;
            }
            return new V50(byte0, remainingLength,
                    packetIdentifier, reasonCode, properties);
        }


        public static V50 from(short packetIdentifier, byte reasonCode, Properties properties) {
            V50 ret = new V50(BYTE_0, calcRemainingLength(reasonCode, properties),
                    packetIdentifier, reasonCode, properties);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return  ret;
        }

        public static V50 from(short packetIdentifier) {
            return from(packetIdentifier, REASON_CODE_SUCCESS, Properties.EMPTY);
        }

        private static int calcRemainingLength(byte reasonCode, Properties properties) {
            if (reasonCode == REASON_CODE_SUCCESS && properties.isEmpty()) {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
                // and there are no Properties. In this case the PUBACK has a Remaining Length of 2.
                return 2;
            }
            int propertyLength = properties.calcPropertyLength();
            // Packet Identifier + Reason Code + Properties
            return 2 + 1 + variableByteIntegerLength(propertyLength) + propertyLength;
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toByteBuf();
            if (remainingLength == 0x02) {
                return buf;
            }
            writeByte(buf, reasonCode);
            writeProperties(buf, properties);
            return buf;
        }

        @Override
        protected boolean packetValidate() {
            return super.packetValidate()
                    && validateReasonCode()
                    && validateProperties();
        }

        private boolean validateReasonCode() {
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

        private boolean validateProperties() {
            if (this.properties == null) {
                return true;
            }
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
