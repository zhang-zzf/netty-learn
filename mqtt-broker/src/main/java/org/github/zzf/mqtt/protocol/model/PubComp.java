package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;

public class PubComp extends ControlPacket {

    private final short packetIdentifier;

    PubComp(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = readPacketIdentifier(incoming);
    }

    public PubComp(short packetIdentifier) {
        this(packetIdentifier, 0x02);
    }

    public PubComp(short packetIdentifier, int remainingLength) {
        super((byte) 0x70, remainingLength);
        this.packetIdentifier = packetIdentifier;
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
        sb.append("\"packetIdentifier\":\"").append(pId()).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }

    public short packetIdentifier() {
        return packetIdentifier;
    }

    public static class V50 extends PubComp {

        final byte reasonCode;
        final Properties properties;

        V50(ByteBuf incoming) {
            super(incoming);
            if (incoming.isReadable()) {
                this.reasonCode = readByte(incoming);
                this.properties = readProperties(incoming);
            }
            else {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
                // and there are no Properties. In this case the PUBACK has a Remaining Length of 2.
                reasonCode = REASON_CODE_SUCCESS;
                properties = Properties.EMPTY;
            }
        }

        V50(short packetIdentifier) {
            this(packetIdentifier, REASON_CODE_SUCCESS, Properties.EMPTY);
        }

        V50(short packetIdentifier, byte reasonCode, Properties properties) {
            super(packetIdentifier, calcRemainingLength(reasonCode, properties));
            this.reasonCode = reasonCode;
            this.properties = properties;
            if (!packetValidate()) {
                throw new MalformedPacketException();
            }
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
                    && validatePubCompReasonCode(reasonCode)
                    && validateProperties();
        }

        boolean validatePubCompReasonCode(byte reasonCode) {
            return reasonCode == REASON_CODE_SUCCESS
                    || reasonCode == REASON_CODE_PACKET_ID_NOT_FOUND;
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
