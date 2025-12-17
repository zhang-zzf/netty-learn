package org.github.zzf.mqtt.mqtt.broker;

import org.github.zzf.mqtt.protocol.session.Session;

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
