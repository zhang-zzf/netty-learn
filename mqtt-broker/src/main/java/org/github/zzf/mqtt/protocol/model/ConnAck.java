package org.github.zzf.mqtt.protocol.model;

import io.netty.buffer.ByteBuf;

public class ConnAck extends ControlPacket {

    public static final byte ACCEPTED = 0x00;
    public static final byte UNACCEPTED_PROTOCOL_VERSION = 0x01;
    public static final byte IDENTIFIER_REJECTED = 0x02;
    public static final byte SERVER_UNAVAILABLE = 0x03;
    public static final byte NOT_AUTHORIZED = 0x05;

    /**
     * Session Present Flag
     */
    final boolean sp;
    final int returnCode;

    public static ConnAck incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        boolean sp = readByte(incoming) != 0x00;
        byte returnCode = readByte(incoming);
        return new ConnAck(byte0, remainingLength,
                sp, returnCode);
    }

    public static ConnAck authenticateFailed(int returnCode) {
        if (returnCode == ACCEPTED) {
            throw new IllegalArgumentException();
        }
        return new ConnAck(false, returnCode);
    }

    private ConnAck(byte byte0, int remainingLength,
            boolean sp, byte returnCode) {
        super(byte0, remainingLength);
        this.sp = sp;
        this.returnCode = returnCode;
    }

    private ConnAck(boolean sp, int returnCode) {
        super((byte) 0x20, 0x02);
        // If a server sends a CONNACK packet containing a non-zero return code
        // it MUST set Session Present to 0
        if (returnCode != 0 || sp) {
            throw new IllegalArgumentException();
        }
        this.sp = sp;
        this.returnCode = returnCode;
    }


    public static ConnAck accepted() {
        return new ConnAck(false, ACCEPTED);
    }

    /**
     * 0x01 Connection Refused, unacceptable protocol version
     */
    public static ConnAck notSupportProtocolLevel() {
        return new ConnAck(false, 0x01);
    }

    public static ConnAck serverUnavailable() {
        return new ConnAck(false, 0x03);
    }

    public static ConnAck acceptedWithStoredSession() {
        return new ConnAck(true, 0x00);
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        buf.writeByte(sp ? 0x01 : 0x00);
        buf.writeByte(returnCode);
        return buf;
    }

    public boolean sp() {
        return this.sp;
    }

    public int returnCode() {
        return this.returnCode;
    }

    public boolean connectionAccepted() {
        return this.returnCode == ACCEPTED;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
        sb.append("\"sp\":").append(sp).append(',');
        sb.append("\"returnCode\":").append(returnCode).append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}
