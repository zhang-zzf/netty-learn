package org.example.mqtt.broker.model;

import io.netty.buffer.ByteBuf;
import org.example.mqtt.model.Publish;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-17
 */
public class BrokerPublish extends Publish {
    /**
     * inbound packet convert to model
     *
     * @param incoming inbound packet
     */
    public BrokerPublish(ByteBuf incoming) {
        super(incoming);
    }
}
