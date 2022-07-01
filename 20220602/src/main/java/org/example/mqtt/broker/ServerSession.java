package org.example.mqtt.broker;

import org.example.mqtt.model.ControlPacket;

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

}
