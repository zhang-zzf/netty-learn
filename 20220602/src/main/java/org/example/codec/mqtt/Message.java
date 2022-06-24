package org.example.codec.mqtt;

import io.netty.buffer.ByteBuf;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/24
 */
public interface Message {

    /**
     * packet identifier of the message
     *
     * @return packet identifier
     */
    int packetIdentifier();

    /**
     * payload
     *
     * @return payload
     */
    ByteBuf payload();

    /**
     * topic
     *
     * @return topic
     */
    String topic();

    /**
     * Quality of Service
     *
     * @return qos
     */
    int qos();

    /**
     *
     */
    void resetPocketIdentifier(short pocketIdentifier);

}
