package org.example.codec.mqtt;

import io.netty.buffer.ByteBuf;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/24
 */
public interface Message {

    /**
     * topic
     *
     * @return topic
     */
    String topic();

    /**
     * packet identifier of the message
     *
     * @return packet identifier
     */
    int packetIdentifier();

    /**
     * Quality of Service
     *
     * @return qos
     */
    int qos();

    /**
     * payload
     *
     * @return payload
     */
    ByteBuf payload();

    /**
     *
     * @param pocketIdentifier
     */
    void resetPocketIdentifier(short pocketIdentifier);

}
