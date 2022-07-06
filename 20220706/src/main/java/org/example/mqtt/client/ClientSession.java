package org.example.mqtt.client;

import org.example.mqtt.session.Session;
import org.example.mqtt.model.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
public interface ClientSession extends Session {

    boolean syncConnect(Connect connect);

    boolean syncSubscribe(Subscribe subscribe);

    boolean syncUnSubscribe(Unsubscribe unsubscribe);

    boolean syncSend(Publish publish);

}
