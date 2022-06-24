package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Getter
@Setter
@Accessors(chain = true)
public class Connect extends ControlPacket {

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

    public boolean cleanSession() {
        return (connectFlags & 0x02) != 0;
    }

    @Override
    public boolean packetValidate() {
        if (_0Byte() != 0x10) {
            return false;
        }
        // If the protocol name is incorrect the Server MAY disconnect the Client.
        // we decide to disconnect the Client
        if (!"MQTT".equals(protocolName)) {
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
        return true;
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

}
