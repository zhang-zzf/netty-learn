package org.example.mqtt.client;

import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
public abstract class Client {

    abstract ClientSession clientSession();

    /**
     * 发起 Connect 请求
     * <p>同步接口</p>
     *
     * @param connect ControlPacket
     */
    public boolean connect(Connect connect) {
        ClientSession session = clientSession();
        return session.syncConnect(connect);
    }


}
