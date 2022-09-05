package org.example.mqtt.client;

import org.example.mqtt.model.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
public abstract class Client implements AutoCloseable {

    abstract ClientSession clientSession();

    abstract String clientIdentifier();

    /**
     * 发起 Connect 请求
     * <p>同步接口</p>
     *
     * @param connect ControlPacket
     */
    public boolean connect(Connect connect) {
        return clientSession().syncConnect(connect);
    }

    public void subscribe(Subscribe subscribe) {
        clientSession().syncSubscribe(subscribe);
    }

    public void unsubscribe(Unsubscribe unsubscribe) {
        clientSession().syncUnSubscribe(unsubscribe);
    }

    public void send(Publish publish) {
    }

    @Override
    public void close() throws Exception {
        // clientSession().send(Disconnect.from()).sync();
        clientSession().closeChannel();
    }

    public void messageReceived(Publish packet) {

    }

}
