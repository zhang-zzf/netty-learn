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
     *
     * @param force force clean Session from Broker if true
     */
    void close(boolean force);

}
