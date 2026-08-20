package org.github.zzf.mqtt.protocol.model;

import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.AUTHENTICATION_DATA;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.AUTHENTICATION_METHOD;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.CONTENT_TYPE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.CORRELATION_DATA;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.MAXIMUM_PACKET_SIZE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.MESSAGE_EXPIRY_INTERVAL;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.PAYLOAD_FORMAT_INDICATOR;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RECEIVE_MAXIMUM;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REQUEST_PROBLEM_INFORMATION;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.REQUEST_RESPONSE_INFORMATION;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.RESPONSE_TOPIC;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.SESSION_EXPIRY_INTERVAL;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.TOPIC_ALIAS_MAXIMUM;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.USER_PROPERTY;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.Properties.WILL_DELAY_INTERVAL;

import io.netty.buffer.ByteBuf;
import java.util.Set;

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
    public static final byte PROTOCOL_LEVEL_5_0 = (byte) 5;
    public static final int VARIABLE_HEADER_LENGTH = 10;
    final String protocolName;
    final byte protocolLevel;
    final byte connectFlags;
    final int keepAlive;
    final String clientIdentifier;
    final String willTopic;
    final ByteBuf willMessage;
    final String username;
    final ByteBuf password;

    private Connect(byte _0byte, int remainingLength,
            String protocolName, byte protocolLevel, byte connectFlags, int keepAlive,
            String clientIdentifier, String willTopic, ByteBuf willMessage, String username, ByteBuf password) {
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

    public static Connect from(String clientIdentifier, short keepAlive) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1, (byte) 0x02, keepAlive,
                clientIdentifier, null, null, null, null);
    }

    public static Connect from(String clientIdentifier, boolean cleanSession, short keepAlive) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1,
                cleanSession, false, 0, false,
                false, false,
                keepAlive,
                clientIdentifier, null, null, null, null);
    }

    public static Connect from(int willQos,
            boolean willRetain,
            short keepAlive,
            String clientIdentifier,
            String willTopic,
            ByteBuf willMessage) {
        return from(PROTOCOL_NAME, PROTOCOL_LEVEL_3_1_1,
                true, true, willQos, willRetain, false, false,
                keepAlive,
                clientIdentifier,
                willTopic, willMessage,
                null, null);
    }

    public static Connect from(
            String protocolName, byte protocolLevel,
            boolean cleanSession, boolean willFlag, int willQos, boolean willRetain, boolean passwordFlag,
            boolean usernameFlag,
            int keepAlive,
            String clientIdentifier, String willTopic, ByteBuf willMessage, String username, ByteBuf password) {
        if (willQos < 0 || willQos > 2) {
            throw new IllegalArgumentException("willQoS is illegal");
        }
        byte connectFlags = 0;
        if (cleanSession) {
            connectFlags |= 0x02;
        }
        if (willFlag) {
            connectFlags |= 0x04;
            connectFlags |= (byte) (willQos << 3);
        }
        if (willRetain) {
            connectFlags |= 0x20;
        }
        if (passwordFlag) {
            connectFlags |= 0x40;
        }
        if (usernameFlag) {
            connectFlags |= (byte) 0x80;
        }
        return from(protocolName, protocolLevel, connectFlags, keepAlive,
                clientIdentifier, willTopic, willMessage, username, password);
    }

    public static Connect from(
            String protocolName, byte protocolLevel, byte connectFlags, int keepAlive,
            String clientIdentifier, String willTopic, ByteBuf willMessage, String username, ByteBuf password) {
        int remainingLength = VARIABLE_HEADER_LENGTH
                + (clientIdentifier == null ? 2 : calcUTF8StringLength(clientIdentifier))
                + (willTopic == null ? 0 : calcUTF8StringLength(willTopic))
                + (willMessage == null ? 0 : calcBinaryDataLength(willMessage))
                + (username == null ? 0 : calcUTF8StringLength(username))
                + (password == null ? 0 : calcBinaryDataLength(password));
        Connect ret = new Connect(CONNECT, remainingLength,
                protocolName, protocolLevel, connectFlags, keepAlive,
                clientIdentifier, willTopic, willMessage, username, password);
        if (!ret.packetValidate()) {
            throw new MalformedPacketException();
        }
        return ret;
    }

    public static Connect incoming(ByteBuf incoming) {
        byte byte0 = readByte(incoming);
        int remainingLength = readVariableByteInteger(incoming);
        // variable header
        String protocolName = readUTF8String(incoming);
        byte protocolLevel = readByte(incoming);
        byte connectFlags = readByte(incoming);
        int keepAlive = readTwoByteInteger(incoming);
        // payload
        String clientIdentifier = readUTF8String(incoming);
        String willTopic;
        ByteBuf willMessage;
        if (willFlag(connectFlags)) {
            willTopic = readUTF8String(incoming);
            // heapBuffer no memory leak
            willMessage = readBinaryData(incoming);
        }
        else {
            willTopic = null;
            willMessage = null;
        }
        String username = usernameFlag(connectFlags) ? readUTF8String(incoming) : null;
        ByteBuf password = passwordFlag(connectFlags) ? readBinaryData(incoming) : null;
        return new Connect(byte0, remainingLength,
                protocolName, protocolLevel, connectFlags, keepAlive,
                clientIdentifier, willTopic, willMessage, username, password);
    }

    static boolean passwordFlag(byte connectFlags) {
        return (connectFlags & 0x40) != 0;
    }

    static boolean usernameFlag(byte connectFlags) {
        return (connectFlags & 0x80) != 0;
    }

    public static boolean willFlag(byte connectFlags) {
        return (connectFlags & 0x04) != 0;
    }

    ByteBuf toPacketByteBuf() {
        return super.toByteBuf();
    }

    @Override
    public ByteBuf toByteBuf() {
        ByteBuf buf = super.toByteBuf();
        // Variable Header
        writeUTF8String(buf, protocolName);
        writeByte(buf, protocolLevel);
        writeByte(buf, connectFlags);
        writeTwoByteInteger(buf, keepAlive);
        // Payload
        writeUTF8String(buf, clientIdentifier);
        if (willFlag()) {
            writeUTF8String(buf, willTopic);
            writeBinaryData(buf, willMessage);
        }
        if (usernameFlag()) {
            writeUTF8String(buf, username);
        }
        if (passwordFlag()) {
            writeBinaryData(buf, password);
        }
        // all direct ByteBuf
        return buf;
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
        if (!validateProtocolLevel()) {
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

    boolean validateProtocolLevel() {
        return protocolLevel == PROTOCOL_LEVEL_3_1_1;
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
        return passwordFlag(connectFlags);
    }

    public boolean usernameFlag() {
        return usernameFlag(connectFlags);
    }

    public boolean willFlag() {
        return willFlag(connectFlags);
    }

    public byte protocolLevel() {
        return protocolLevel;
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


    public static class V50 extends Connect {
        public static final byte PROTOCOL_LEVEL_5_0 = (byte) 5;

        final Properties properties;
        final Properties willProperties;
        final Set<Integer> willMessageAllowedProperties = Set.of(
                WILL_DELAY_INTERVAL,
                PAYLOAD_FORMAT_INDICATOR,
                MESSAGE_EXPIRY_INTERVAL,
                CONTENT_TYPE,
                RESPONSE_TOPIC,
                CORRELATION_DATA,
                USER_PROPERTY
        );
        final Set<Integer> allowedProperties = Set.of(
                SESSION_EXPIRY_INTERVAL,
                RECEIVE_MAXIMUM,
                MAXIMUM_PACKET_SIZE,
                TOPIC_ALIAS_MAXIMUM,
                REQUEST_RESPONSE_INFORMATION,
                REQUEST_PROBLEM_INFORMATION,
                USER_PROPERTY,
                AUTHENTICATION_METHOD,
                AUTHENTICATION_DATA
        );

        private V50(byte _0byte, int remainingLength,
                String protocolName, byte protocolLevel, byte connectFlags, int keepAlive, Properties properties,
                String clientIdentifier, Properties willProperties, String willTopic, ByteBuf willMessage,
                String username, ByteBuf password) {
            super(_0byte, remainingLength,
                    protocolName, protocolLevel, connectFlags, keepAlive,
                    clientIdentifier, willTopic, willMessage, username, password);
            this.properties = properties;
            this.willProperties = willProperties;
        }

        public static V50 incoming(ByteBuf incoming) {
            byte byte0 = readByte(incoming);
            int remainingLength = readVariableByteInteger(incoming);
            // variable header
            String protocolName = readUTF8String(incoming);
            byte protocolLevel = readByte(incoming);
            byte connectFlags = readByte(incoming);
            int keepAlive = readTwoByteInteger(incoming);
            Properties properties = readProperties(incoming);
            // payload
            String clientIdentifier = readUTF8String(incoming);
            Properties willProperties;
            String willTopic;
            ByteBuf willMessage;
            if (willFlag(connectFlags)) {
                willProperties = readProperties(incoming);
                willTopic = readUTF8String(incoming);
                // heapBuffer no memory leak
                willMessage = readBinaryData(incoming);
            }
            else {
                willProperties = Properties.EMPTY;
                willTopic = null;
                willMessage = null;
            }
            String username = usernameFlag(connectFlags) ? readUTF8String(incoming) : null;
            ByteBuf password = passwordFlag(connectFlags) ? readBinaryData(incoming) : null;
            return new V50(byte0, remainingLength,
                    protocolName, protocolLevel, connectFlags, keepAlive, properties,
                    clientIdentifier, willProperties, willTopic, willMessage, username, password);
        }

        public static V50 from(
                String protocolName, byte protocolLevel,
                boolean cleanSession, boolean willFlag, int willQos, boolean willRetain, boolean passwordFlag,
                boolean usernameFlag,
                int keepAlive, Properties properties,
                String clientIdentifier, Properties willProperties, String willTopic, ByteBuf willMessage,
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
                connectFlags |= (byte) (willQos << 3);
            }
            if (willRetain) {
                connectFlags |= 0x20;
            }
            if (passwordFlag) {
                connectFlags |= 0x40;
            }
            if (usernameFlag) {
                connectFlags |= (byte) 0x80;
            }
            return from(protocolName, protocolLevel, connectFlags, keepAlive, properties,
                    clientIdentifier, willProperties, willTopic, willMessage, username, password);
        }

        public static V50 from(
                String protocolName, byte protocolLevel, byte connectFlags, int keepAlive, Properties properties,
                String clientIdentifier, Properties willProperties, String willTopic, ByteBuf willMessage,
                String username, ByteBuf password) {
            int remainingLength = VARIABLE_HEADER_LENGTH + calcPropertiesLength(properties)
                    + (clientIdentifier == null ? 2 : calcUTF8StringLength(clientIdentifier))
                    + (willProperties == null ? 0 : calcPropertiesLength(willProperties))
                    + (willTopic == null ? 0 : calcUTF8StringLength(willTopic))
                    + (willMessage == null ? 0 : calcBinaryDataLength(willMessage))
                    + (username == null ? 0 : calcUTF8StringLength(username))
                    + (password == null ? 0 : calcBinaryDataLength(password));
            V50 ret = new V50(CONNECT, remainingLength,
                    protocolName, protocolLevel, connectFlags, keepAlive, properties,
                    clientIdentifier, willProperties, willTopic, willMessage, username, password);
            if (!ret.packetValidate()) {
                throw new MalformedPacketException();
            }
            return ret;
        }

        @Override
        public ByteBuf toByteBuf() {
            ByteBuf buf = toPacketByteBuf();
            // Variable Header
            writeUTF8String(buf, protocolName);
            writeByte(buf, protocolLevel);
            writeByte(buf, connectFlags);
            writeTwoByteInteger(buf, keepAlive);
            writeProperties(buf, properties);
            // Payload
            writeUTF8String(buf, clientIdentifier);
            if (willFlag()) {
                writeProperties(buf, willProperties);
                writeUTF8String(buf, willTopic);
                writeBinaryData(buf, willMessage);
            }
            if (usernameFlag()) {
                writeUTF8String(buf, username);
            }
            if (passwordFlag()) {
                writeBinaryData(buf, password);
            }
            // all direct ByteBuf
            return buf;
        }

        @Override
        public boolean packetValidate() {
            return super.packetValidate()
                    && properties.validateIdentifier(allowedProperties)
                    && willProperties.validateIdentifier(willMessageAllowedProperties);
        }

        @Override
        boolean validateProtocolLevel() {
            return protocolLevel == PROTOCOL_LEVEL_5_0;
        }

        public Properties properties() {
            return this.properties;
        }

        public boolean cleanStart() {
            return cleanSession();
        }

        public long sessionExpiryInterval() {
            return properties.sessionExpiryInterval().orElse(0L);
        }

    }


}