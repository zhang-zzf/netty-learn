package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/23
 */
public interface ServerSession extends Session {

    /**
     * the broker that session was bound to
     *
     * @return Broker
     */
    Broker broker();

    /**
     * register the session to broker
     *
     * @param broker broker
     */
    void register(Broker broker);

    /**
     * deregister the session from broker
     */
    void deregister();

    /**
     * whether the Session is registered with a Broker.
     *
     * @return Returns {@code true} if the {@link Session} is registered with a {@link Broker}.
     */
    boolean isRegistered();

    /**
     * register from Broker and bind to the Channel
     */
    void open(Channel ch, Broker broker);

    /**
     * deregister from Broker and close the Channel
     */
    void close();

}
