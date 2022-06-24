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

    @Override
    protected void initPacket() {
        if (type() != 1) {
            throw new IllegalArgumentException();
        }
        buf.markReaderIndex();
        try {
            buf.skipBytes(fixedHeaderByteCnt());
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
        } finally {
            buf.resetReaderIndex();
        }

    }

    private boolean passwordFlag() {
        return false;
    }

    private boolean usernameFlag() {
        return false;
    }

    public boolean willFlag() {
        return false;
    }

}
