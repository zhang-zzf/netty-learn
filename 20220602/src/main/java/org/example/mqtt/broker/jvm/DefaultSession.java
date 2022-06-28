package org.example.mqtt.broker.jvm;

import io.netty.channel.Channel;
import org.example.mqtt.broker.AbstractBroker;
import org.example.mqtt.broker.AbstractSession;
import org.example.mqtt.broker.ControlPacketContext;
import org.example.mqtt.broker.Topic;

import java.util.Deque;
import java.util.LinkedList;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
public class DefaultSession extends AbstractSession {

    private Deque<ControlPacketContext> inQueue = new LinkedList<>();
    private Deque<ControlPacketContext> outQueue = new LinkedList<>();

    protected DefaultSession(Channel channel, AbstractBroker broker) {
        super(channel, broker);
    }

    @Override
    protected Deque<ControlPacketContext> inQueue() {
        return inQueue;
    }

    @Override
    protected Deque<ControlPacketContext> outQueue() {
        return outQueue;
    }

    @Override
    public Integer subscriptionQos(Topic topic) {
        throw new UnsupportedOperationException();
    }

}
