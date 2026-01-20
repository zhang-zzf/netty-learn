package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class PubAck extends ControlPacket {

    final short packetIdentifier;

    PubAck(ByteBuf incoming) {
        super(incoming);
        this.packetIdentifier = incoming.readShort();
    }

    public PubAck(short packetIdentifier) {
        this(packetIdentifier, 0x02);
    }

    public PubAck(short packetIdentifier, int remainingLength) {
        super((byte) 0x40, remainingLength);
        this.packetIdentifier = packetIdentifier;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        buf.writeShort(packetIdentifier);
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

        final byte reasonCode;
        final Properties properties;

        V50(ByteBuf incoming) {
            super(incoming);
            if (incoming.isReadable()) {
                this.reasonCode = readByte(incoming);
                this.properties = Properties.incoming(incoming.readSlice(readVariableByteInteger(incoming)));
            }
            else {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
                // and there are no Properties. In this case the PUBACK has a Remaining Length of 2.
                reasonCode = REASON_CODE_SUCCESS;
                properties = Properties.EMPTY;
            }
        }

        V50(short packetIdentifier, byte reasonCode, Properties properties) {
            super(packetIdentifier, calcRemainingLength(reasonCode, properties));
            this.reasonCode = reasonCode;
            this.properties = properties;
        }

        private static int calcRemainingLength(byte reasonCode, Properties properties) {
            if (reasonCode == REASON_CODE_SUCCESS && properties.isEmpty()) {
                // The Reason Code and Property Length can be omitted if the Reason Code is 0x00 (Success)
                // and there are no Properties. In this case the PUBACK has a Remaining Length of 2.
                return 2;
            }
            int propertyLength = properties.calcPropertyLength();
            // Packet Identifier + PUBACK Reason Code + Properties
            return 2 + 1 + variableByteIntegerLength(propertyLength) + propertyLength;
        }

        V50(short packetIdentifier) {
            this(packetIdentifier, REASON_CODE_SUCCESS, Properties.EMPTY);
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = super.toByteBuf();
            if (remainingLength == 0x02) {
                return buf;
            }
            writeByte(buf, reasonCode);
            buf.writeBytes(properties.writeToByteBuf(buf));
            return buf;
        }

    }
}
