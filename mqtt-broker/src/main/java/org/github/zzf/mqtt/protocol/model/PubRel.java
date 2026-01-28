package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_PACKET_ID_NOT_FOUND;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.ControlPacketV50.REASON_CODE_SUCCESS;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REASON_STRING;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;

import io.netty.buffer.ByteBuf;
import java.util.Set;

public class PubRel extends ControlPacket {

    public static final byte BYTE_0 = (byte) 0x60;
    final short packetIdentifier;

    private PubRel(byte byte0, int remainingLength, short packetIdentifier) {
        super(byte0, remainingLength);
        this.packetIdentifier = packetIdentifier;
    }

    public static PubRel incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        short packetIdentifier = readPacketIdentifier(incoming);
        return new PubRel(byte0, remainingLength, packetIdentifier);
    }

    public static PubRel from(short packetIdentifier) {
        return from(packetIdentifier, 0x02);
    }

    public static PubRel from(short packetIdentifier, int remainingLength) {
        PubRel ret = new PubRel(BYTE_0, remainingLength, packetIdentifier);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        writeTwoByteInteger(buf, packetIdentifier);
        return buf;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"packetIdentifier\":\"0x").append(Integer.toHexString(packetIdentifier & 0xffff)).append("\",");
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public String pId() {
        return hexPId(packetIdentifier);
    }


    public static class V50 extends PubRel {

        final byte reasonCode;
        final Properties properties;
        final Set<Integer> SUPPORTED_PROPERTY_IDENTIFIERS = Set.of(REASON_STRING, USER_PROPERTY);

        V50(byte byte0, int remainingLength,
                short packetIdentifier, byte reasonCode, Properties properties) {
            super(byte0, remainingLength, packetIdentifier);
            this.reasonCode = reasonCode;
            this.properties = properties;
        }

        public static V50 from(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            short packetIdentifier = readPacketIdentifier(incoming);
            byte reasonCode;
            Properties properties;
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

        public static V50 from(short packetIdentifier) {
            return from(packetIdentifier, REASON_CODE_SUCCESS, Properties.EMPTY);
        }

        public static V50 from(short packetIdentifier, byte reasonCode, Properties properties) {
            V50 ret = new V50(BYTE_0, calcRemainingLength(reasonCode, properties),
                    packetIdentifier, reasonCode, properties);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return ret;
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
                    && validatePubRelReasonCode(reasonCode)
                    && properties.validateIdentifier(SUPPORTED_PROPERTY_IDENTIFIERS);
        }

        boolean validatePubRelReasonCode(byte reasonCode) {
            return reasonCode == REASON_CODE_SUCCESS
                    || reasonCode == REASON_CODE_PACKET_ID_NOT_FOUND;
        }

    }

}
