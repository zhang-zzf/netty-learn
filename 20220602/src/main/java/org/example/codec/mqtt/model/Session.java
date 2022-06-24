package org.example.codec.mqtt.model;

import io.netty.channel.Channel;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Queue;
import java.util.Set;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
@Data
@Accessors(chain = true)
public class Session {

    private String clientId;
    private boolean persistent;
    /**
     * out queue
     */
    private Queue<Message> outQueue;
    /**
     * in queue
     */
    private Queue<Message> inQueue;
    /**
     * subscript
     */
    private Set<Subscription> subscriptionSet;

    /**
     * the Channel that bind to the session.
     * <p>null for no connection between the server and the client</p>
     */
    private Channel channel;

}
