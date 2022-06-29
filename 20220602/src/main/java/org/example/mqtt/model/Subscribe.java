package org.example.mqtt.model;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public class Subscribe extends ControlPacket {

    private short packetIdentifier;
    private List<Subscription> subscriptions;

    public Subscribe(ByteBuf buf) {
        super(buf);
    }

    public List<Subscription> subscriptions() {
        return this.subscriptions;
    }

    public short packetIdentifier() {
        return this.packetIdentifier;
    }

    @Override
    protected void initPacket() {
        final ByteBuf buf = this.buf;
        this.packetIdentifier = buf.readShort();
        this.subscriptions = new ArrayList<>();
        try {
            while (buf.isReadable()) {
                String topic = buf.readCharSequence(buf.readShort(), UTF_8).toString();
                byte qos = buf.readByte();
                this.subscriptions.add(new Subscription(topic, qos));
            }
        } catch (Exception e) {
            //
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean packetValidate() {
        // Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control Packet are reserved and MUST be set to
        // 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection
        if (this._0byte != 0x82) {
            return false;
        }
        //  The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair.
        if (subscriptions.isEmpty()) {
            return false;
        }
        for (Subscription sub : subscriptions) {
            int qos = sub.qos();
            // The Server MUST treat a SUBSCRIBE packet as malformed and close the
            // Network Connection if any of Reserved bits in the payload are non-zero, or QoS is not 0,1 or 2
            if ((qos & 0xFC) != 0) {
                return false;
            }
            if (qos == 0 || qos == 1 || qos == 2) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * @author zhanfeng.zhang
     * @date 2022/06/23
     */
    @Accessors(chain = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Subscription {

        private String topicFilter;
        private int qos;

        public String topicFilter() {
            return this.topicFilter;
        }

        public int qos() {
            return this.qos;
        }


    }

}
