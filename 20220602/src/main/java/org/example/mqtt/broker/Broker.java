package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/24
 */
public interface Broker extends AutoCloseable {

    /**
     * onward message
     *
     * @param packet data
     */
    void onward(Publish packet);

    /**
     * register a session to the broker
     *
     * @param packet data
     * @param channel Channel
     * @return session session
     */
    Session accepted(Connect packet, Channel channel) throws Exception;

    /**
     * deregister a session from the broker
     *
     * @param session Session
     */
    void disconnect(Session session);

    /**
     * register a subscription between the session and the topic
     *
     * @param subscriptions the topic and the qos
     */
   List<Subscription> register(List<Subscription> subscriptions);

    /**
     * deregister a subscription between the session and the topic
     *
     * @param subscriptions the topic and the qos
     */
    void deregister(List<Subscription> subscriptions);

    Set<Integer> supportProtocolLevel();

}
