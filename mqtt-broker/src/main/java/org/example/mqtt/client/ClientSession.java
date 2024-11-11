package org.example.mqtt.client;

import org.example.mqtt.session.Session;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-11
 */
public interface ClientSession extends Session {

    int keepAlive();

}
