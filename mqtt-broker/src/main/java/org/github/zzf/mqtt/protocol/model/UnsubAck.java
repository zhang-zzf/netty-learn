package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.*;
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

