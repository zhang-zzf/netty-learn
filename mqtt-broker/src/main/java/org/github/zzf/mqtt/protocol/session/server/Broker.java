package org.github.zzf.mqtt.protocol.session.server;

import io.netty.channel.Channel;
import java.util.Collection;
import java.util.List;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
public interface Broker {

    /**
     * Connect Event
     */
    ServerSession onConnect(Connect connect, Channel channel);

    /**
     * Publish Event
     *
     * @param packet ControlPacket
     */
    int forward(Publish packet);

    /**
     * onward message
     *
     * @param packet data
     */
    // int forward(Publish packet);

    /**
     * register a subscription between the session and the topic
     */
    List<Subscribe.Subscription> subscribe(ServerSession session, Collection<Subscription> subscriptions);

    /**
     * deregister a subscription between the session and the topic
     */
    void unsubscribe(ServerSession session, Collection<Subscription> subscriptions);

    // ServerSession session(String clientIdentifier);

    void close() throws Exception;

    // todo
    // default boolean closed() {
    //     return false;
    // }

}
