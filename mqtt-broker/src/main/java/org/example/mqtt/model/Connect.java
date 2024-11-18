package org.example.mqtt.model;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-17
 */
public class Connect extends ControlPacket {

    public static final String PROTOCOL_NAME = "MQTT";
    /**
     * version 3.1.1
     */
    public static final byte PROTOCOL_LEVEL_3_1_1 = (byte) 4;
    public static final int VARIABLE_HEADER_LENGTH = 10;
    private String protocolName;
    private byte protocolLevel;
    private byte connectFlags;
    private short keepAlive;
    private String clientIdentifier;
    private String willTopic;
    private ByteBuf willMessage;
    private String username;
    private ByteBuf password;


    private int payloadLength = 0;

    Connect(ByteBuf incoming) {
        super(incoming);
        protocolName = incoming.readCharSequence(incoming.readShort(), UTF_8).toString();
        protocolLevel = incoming.readByte();
        connectFlags = incoming.readByte();
        keepAlive = incoming.readShort();
        short cIdLng = incoming.readShort();
        clientIdentifier = incoming.readCharSequence(cIdLng, UTF_8).toString();
        payloadLength += 2 + cIdLng;
        if (willFlag()) {
            short willFlagLng = incoming.readShort();
            willTopic = incoming.readCharSequence(willFlagLng, UTF_8).toString();
            payloadLength += 2 + willFlagLng;
            // heapBuffer no memory leak
            willMessage = PooledByteBufAllocator.DEFAULT.heapBuffer(incoming.readShort());
            incoming.readBytes(willMessage);
            payloadLength += willMessage.readableBytes();
        }
        if (usernameFlag()) {
            short userNameLng = incoming.readShort();
            username = incoming.readCharSequence(userNameLng, UTF_8).toString();
            payloadLength += 2 + userNameLng;
        }
        if (passwordFlag()) {
            // heapBuffer no memory leak
            password = PooledByteBufAllocator.DEFAULT.heapBuffer(incoming.readShort());
            incoming.readBytes(password);
            payloadLength += password.readableBytes();
        }
    }

    public static Connect from(String clientIdentifier, short keepAlive) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1, (byte) 0x02, keepAlive, clientIdentifier, null, null, null,
            null);
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
        int remainingLength = VARIABLE_HEADER_LENGTH
            + (clientIdentifier == null ? 2 : clientIdentifier.getBytes(UTF_8).length + 2)
            + (willTopic == null ? 0 : willTopic.getBytes(UTF_8).length + 2)
            + (willMessage == null ? 0 : willMessage.readableBytes() + 2)
            + (username == null ? 0 : username.getBytes(UTF_8).length + 2)
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
        this.payloadLength = remainingLength - VARIABLE_HEADER_LENGTH;
        if (!packetValidate()) {
            throw new IllegalArgumentException("Invalid packet");
        }
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf fixedHeader = fixedHeaderByteBuf();
        // variable header
        ByteBuf varHeader = PooledByteBufAllocator.DEFAULT.directBuffer(VARIABLE_HEADER_LENGTH);
        varHeader.writeShort(protocolName.length()); // MQTT
        varHeader.writeCharSequence(protocolName, UTF_8);
        varHeader.writeByte(protocolLevel);
        varHeader.writeByte(connectFlags);
        varHeader.writeShort(keepAlive);
        // Payload
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        // Client Identifier
        byte[] cIdBytes = clientIdentifier.getBytes(UTF_8); // 兼容中文
        payload.writeShort(cIdBytes.length);
        payload.writeBytes(cIdBytes);
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
        // all direct ByteBuf
        return Unpooled.compositeBuffer()
            .addComponents(true, fixedHeader, varHeader, payload);
    }

    public int keepAlive() {
        return keepAlive;
    }

    public boolean cleanSession() {
        return (connectFlags & 0x02) != 0;
    }

    public String clientIdentifier() {
        return clientIdentifier;
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
        sb.append("\"packet\":\"").append(this.getClass().getSimpleName().toUpperCase()).append('\"').append(',');
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
            }
            else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            }
            else {
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
            }
            else if (objectStr.startsWith("[") && objectStr.endsWith("]")) {
                sb.append(objectStr);
            }
            else {
                sb.append("\"").append(objectStr).append("\"");
            }
            sb.append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

}