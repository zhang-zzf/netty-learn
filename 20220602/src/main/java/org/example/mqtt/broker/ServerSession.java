package org.example.mqtt.broker;

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
     * whether the Session is registered with a Broker.
     *
     * @return Returns {@code true} if the {@link Session} is registered with a {@link Broker}.
     */
    boolean isRegistered();

}
