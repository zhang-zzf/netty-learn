package org.example.mqtt.client;

import io.netty.buffer.ByteBuf;
import org.example.mqtt.model.*;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2024-11-12
 */
public interface Client {

    String clientIdentifier();

    short keepAlive();

    CompletionStage<ConnAck> connect(Connect connect);

    CompletionStage<SubAck> subscribe(List<Subscribe.Subscription> sub);

    CompletionStage<UnsubAck> unsubscribe(List<Subscribe.Subscription> unsub);

    void disconnect();

    void close();

    /**
     * publish a Publish Packet to peer
     * @return a future that will be completed when the Publish Packet send complete
     */
    CompletionStage<Void> publish(int qos, String topicName, ByteBuf payload);

    void onPublish(Publish publish);

}
