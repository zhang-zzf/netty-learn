package org.example.mqtt.client;

import org.example.mqtt.session.Session;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
public interface ClientSession extends Session {


    int keepAlive();

}
