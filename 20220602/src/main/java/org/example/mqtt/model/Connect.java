package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Connect extends ControlPacket {

    public static final String PROTOCOL_NAME = "MQTT";
    /**
     * version 3.1.1
     */
    public static final byte PROTOCOL_LEVEL_3_1_1 = (byte) 4;
    private String protocolName;
    private byte protocolLevel;
    private byte connectFlags;
    private short keepAlive;
    private String clientIdentifier;
    private String willTopic;
    private ByteBuf willMessage;
    private String username;
    private ByteBuf password;

    public Connect(ByteBuf buf) {
        super(buf);
    }

    public static Connect from(String clientIdentifier, short keepAlive) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1, (byte) 0x02, keepAlive, clientIdentifier, null, null, null, null);
    }

    public static Connect from(String protocolName,
                               byte protocolLevel, byte connectFlags, short keepAlive,
                               String clientIdentifier, String willTopic, ByteBuf willMessage,
                               String username, ByteBuf password) {
        int remainingLength = protocolName.length() + 2 + 1 + 1 + 2
                + (clientIdentifier == null ? 2 : clientIdentifier.length() + 2)
                + (willTopic == null ? 0 : willTopic.length() + 2)
                + (willMessage == null ? 0 : willMessage.readableBytes() + 2)
                + (username == null ? 0 : username.length() + 2)
                + (password == null ? 0 : password.readableBytes() + 2);
        return new Connect((byte) 0x10, remainingLength, protocolName,
                protocolLevel, connectFlags, keepAlive, clientIdentifier,
                willTopic, willMessage,
                username, password);
    }

    public Connect(byte _0byte, int remainingLength, String protocolName,
                   byte protocolLevel, byte connectFlags, short keepAlive,
                   String clientIdentifier, String willTopic, ByteBuf willMessage,
                   String username, ByteBuf password) {
        super(_0byte, remainingLength);
        this.protocolName = protocolName;
        this.protocolLevel = protocolLevel;
        this.connectFlags = connectFlags;
        this.keepAlive = keepAlive;
        this.clientIdentifier = clientIdentifier;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.username = username;
        this.password = password;
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf header = fixedHeaderByteBuf();
        header.writeShort(protocolName.length());
        header.writeCharSequence(protocolName, UTF_8);
        header.writeByte(protocolLevel);
        header.writeByte(connectFlags);
        header.writeShort(keepAlive);
        // Payload
        // Client Identifier
        ByteBuf payload = Unpooled.buffer();
        payload.writeShort(clientIdentifier.length());
        payload.writeCharSequence(clientIdentifier, UTF_8);
        if (willFlag()) {
            payload.writeShort(willTopic.length());
            payload.writeCharSequence(willTopic, UTF_8);
            payload.writeShort(willMessage.readableBytes());
            payload.writeBytes(willMessage);
        }
        if (usernameFlag()) {
            payload.writeShort(username.length());
            payload.writeCharSequence(username, UTF_8);
        }
        if (passwordFlag()) {
            payload.writeShort(password.readableBytes());
            payload.writeBytes(password);
        }
        return Unpooled.compositeBuffer()
                .addComponent(true, header)
                .addComponent(true, payload);

    }

    public int keepAlive() {
        return keepAlive;
    }

    public boolean cleanSession() {
        return (connectFlags & 0x02) != 0;
    }

    public String clientIdentifier() {
        return this.clientIdentifier;
    }

    @Override
    public boolean packetValidate() {
        if (_0byte != 0x10) {
            return false;
        }
        // If the protocol name is incorrect the Server MAY disconnect the Client.
        // we decide to disconnect the Client
        if (!PROTOCOL_NAME.equals(protocolName)) {
            return false;
        }
        // The Server MUST validate that the reserved flag in the CONNECT Control Packet is set to zero and
        // disconnect the Client if it is not zero
        if ((connectFlags & 0x01) != 0) {
            return false;
        }
        if (willFlag() && (willTopic == null || willMessage == null)) {
            return false;
        }
        if (!willFlag() && willQos() != 0) {
            return false;
        }
        if (willQos() > 2) {
            return false;
        }
        if (!willFlag() && willReturnFlag()) {
            return false;
        }
        if ((usernameFlag() && username == null)) {
            return false;
        }
        if (!usernameFlag() && username != null) {
            return false;
        }
        if (!usernameFlag() && passwordFlag()) {
            return false;
        }
        if (passwordFlag() && password == null) {
            return false;
        }
        if (!passwordFlag() && password != null) {
            return false;
        }
        if (clientIdentifier == null) {
            return false;
        }
        return super.packetValidate();
    }

    public boolean willReturnFlag() {
        return (connectFlags & 0x20) != 0;
    }

    public int willQos() {
        return (connectFlags & 0x18);
    }

    @Override
    protected void initPacket() {
        protocolName = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        protocolLevel = buf.readByte();
        connectFlags = buf.readByte();
        keepAlive = buf.readShort();
        clientIdentifier = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        if (willFlag()) {
            willTopic = buf.readCharSequence(buf.readShort(), UTF_8).toString();
            willMessage = buf.readSlice(buf.readShort());
        }
        if (usernameFlag()) {
            username = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        }
        if (passwordFlag()) {
            password = buf.readSlice(buf.readShort());
        }
    }

    public boolean passwordFlag() {
        return (connectFlags & 0x40) != 0;
    }

    public boolean usernameFlag() {
        return (connectFlags & 0x80) != 0;
    }

    public boolean willFlag() {
        return (connectFlags & 0x04) != 0;
    }

    public Integer protocolLevel() {
        return Integer.valueOf(protocolLevel);
    }

}
