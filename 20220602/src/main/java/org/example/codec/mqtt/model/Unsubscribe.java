package org.example.codec.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Data
@Accessors(chain = true)
public class Unsubscribe extends ControlPacket {

    private short packetIdentifier;
    private List<Subscription> subscriptionList;

    public Unsubscribe(ByteBuf buf) {
        super(buf);
    }

    @Override
    protected void initPacket() {
        final ByteBuf buf = getBuf();
        this.packetIdentifier = buf.readShort();
        this.subscriptionList = new ArrayList<>();
        try {
            while (buf.isReadable()) {
                String topic = buf.readCharSequence(buf.readShort(), UTF_8).toString();
                this.subscriptionList.add(new Subscription(topic, 0));
            }
        } catch (Exception e) {
            // The Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 encoded strings
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean packetValidate() {
        // Bits 3,2,1 and 0 of the fixed header of the UNSUBSCRIBE Control Packet are reserved and MUST be set to
        // 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
        if (_0Byte() != 0xA2) {
            return false;
        }
        //  The payload of a UNSUBSCRIBE packet MUST contain at least one Topic Filter.
        if (subscriptionList.isEmpty()) {
            return false;
        }
        return true;
    }

}
