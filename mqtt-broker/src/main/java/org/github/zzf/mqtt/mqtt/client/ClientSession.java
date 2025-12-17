package org.github.zzf.mqtt.mqtt.client;

import org.github.zzf.mqtt.protocol.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-11
 */
public interface ClientSession extends Session {

    int keepAlive();

}
