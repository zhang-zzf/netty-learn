package org.github.zzf.mqtt.protocol.server;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Broker {

    ServerSession session(String clientId);

    byte authenticate(Connect connect);

    /**
     * Connect Event
     */
    CompletionStage<Void> connect(ServerSession session);

    CompletionStage<Void> disconnect(ServerSession session);

    /**
     * Publish Event
     *
     * @param packet ControlPacket
     */
    int forward(Publish packet);

    /**
     * register a subscription between the session and the topic
     */
    List<Subscription> subscribe(ServerSession session,
            Collection<Subscription> subscriptions);

    /**
     * deregister a subscription between the session and the topic
     */
    void unsubscribe(ServerSession session,
            Collection<Subscription> subscriptions);

    void close();

    // todo
    // default boolean closed() {
    //     return false;
    // }

}
