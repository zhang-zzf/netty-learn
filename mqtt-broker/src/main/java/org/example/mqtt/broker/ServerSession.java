package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2022/06/23
 */
public interface ServerSession extends Session {

    /**
     * the broker that session was bound to
     *
     * @return Broker
     */
    Broker broker();

}
