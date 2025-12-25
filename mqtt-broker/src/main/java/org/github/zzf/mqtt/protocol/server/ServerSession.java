package org.github.zzf.mqtt.protocol.server;

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

    /**
     * 检查当前会话是否是从先前会话恢复/复用的
     *
     * @return true - 会话是复用的；false - 会话是新建的
     */
    default boolean isResumed() {
        return false;
    }

    ;

}
