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

    public static Connect from(String clientIdentifier, boolean cleanSession, short keepAlive) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1,
                cleanSession, false, 0, false,
                false, false,
                keepAlive,
                clientIdentifier,
                null, null,
                null, null);
    }

    public static Connect from(int willQos, boolean willRetain,
                               short keepAlive,
                               String clientIdentifier,
                               String willTopic, ByteBuf willMessage) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1,
                true, true, willQos, willRetain, false, false,
                keepAlive,
                clientIdentifier,
                willTopic, willMessage,
                null, null);
    }

    public static Connect from(String protocolName, byte protocolLevel,
                               boolean cleanSession, boolean willFlag, int willQos, boolean willRetain,
                               boolean passwordFlag, boolean usernameFlag,
                               short keepAlive,
                               String clientIdentifier,
                               String willTopic, ByteBuf willMessage,
                               String username, ByteBuf password) {
        if (willQos < 0 || willQos > 2) {
            throw new IllegalArgumentException("willQoS is illegal");
        }
        byte connectFlags = 0;
        if (cleanSession) {
            connectFlags |= 0x02;
        }
        if (willFlag) {
            connectFlags |= 0x04;
            connectFlags |= (willQos << 3);
        }
        if (willRetain) {
            connectFlags |= 0x20;
        }
        if (passwordFlag) {
            connectFlags |= 0x40;
        }
        if (usernameFlag) {
            connectFlags |= 0x80;
        }
        return from(protocolName, protocolLevel, connectFlags, keepAlive,
                clientIdentifier, willTopic, willMessage, username, password);
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
        return new Connect(CONNECT, remainingLength, protocolName,
                protocolLevel, connectFlags, keepAlive, clientIdentifier,
                willTopic, willMessage,
                username, password);
    }

    private Connect(byte _0byte, int remainingLength, String protocolName,
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
            byte[] bytes = willTopic.getBytes(UTF_8);
            payload.writeShort(bytes.length);
            payload.writeBytes(bytes);
            payload.writeShort(willMessage.readableBytes());
            payload.writeBytes(willMessage);
        }
        if (usernameFlag()) {
            byte[] bytes = username.getBytes(UTF_8);
            payload.writeShort(bytes.length);
            payload.writeBytes(bytes);
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
        if (byte0 != 0x10) {
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
        if (!willFlag() && willRetainFlag()) {
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

    public boolean willRetainFlag() {
        return (connectFlags & 0x20) != 0;
    }

    public int willQos() {
        return ((connectFlags >> 3) & 0x03);
    }

    public String willTopic() {
        return willTopic;
    }

    public ByteBuf willMessage() {
        return willMessage;
    }

    @Override
    protected void initPacket() {
        ByteBuf buf = content();
        protocolName = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        protocolLevel = buf.readByte();
        connectFlags = buf.readByte();
        keepAlive = buf.readShort();
        clientIdentifier = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        if (willFlag()) {
            willTopic = buf.readCharSequence(buf.readShort(), UTF_8).toString();
            willMessage = Unpooled.buffer(buf.readShort());
            buf.readBytes(willMessage);
        }
        if (usernameFlag()) {
            username = buf.readCharSequence(buf.readShort(), UTF_8).toString();
        }
        if (passwordFlag()) {
            password = Unpooled.buffer(buf.readShort());
            buf.readBytes(password);
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (protocolName != null) {
            sb.append("\"protocolName\":\"").append(protocolName).append('\"').append(',');
        }
        sb.append("\"protocolLevel\":").append(protocolLevel).append(',');
        sb.append("\"connectFlags\":").append(connectFlags).append(',');
        sb.append("\"keepAlive\":").append(keepAlive).append(',');
        if (clientIdentifier != null) {
            sb.append("\"clientIdentifier\":\"").append(clientIdentifier).append('\"').append(',');
        }
        if (willTopic != null) {
            sb.append("\"willTopic\":\"").append(willTopic).append('\"').append(',');
        }
        if (willMessage != null) {
            sb.append("\"willMessage\":");
            String objectStr = willMessage.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            } else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            } else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        if (username != null) {
            sb.append("\"username\":\"").append(username).append('\"').append(',');
        }
        if (password != null) {
            sb.append("\"password\":");
            String objectStr = password.toString().trim();
            if (objectStr.startsWith("{") && objectStr.endsWith("}")) {
                sb.append(objectStr);
            } else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            } else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}