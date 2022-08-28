package org.example.mqtt.broker;

import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.List;
import java.util.concurrent.Future;

public interface BrokerState extends AutoCloseable {

    /**
     * clientIdentifier -> Session
     *
     * @param clientIdentifier id
     * @return Session
     */
    ServerSession session(String clientIdentifier);

    /**
     * topicName match Topic
     *
     * @param topicName name
     * @return Topic
     */
    List<Topic> match(String topicName);

    /**
     * subscribe
     *
     * @param session the Session
     * @param subscription the Subscription
     * @return the Future
     */
    Future<Void> subscribe(ServerSession session, Subscribe.Subscription subscription);

    /**
     * unsubscribe
     *
     * @param session the Session
     * @param subscription the Subscription
     * @return the Future
     */
    Future<Void> unsubscribe(ServerSession session, Subscribe.Subscription subscription);

    /**
     * disconnect
     *
     * @param session Session
     */
    Future<Void> disconnect(ServerSession session);

    /**
     * connect
     *
     * @param session Session
     * @return the old Session that map to the ClientIdentifier. null if not exits.
     */
    Future<ServerSession> connect(ServerSession session);

    void removeRetain(Publish packet);

    void saveRetain(Publish packet);

    List<Publish> matchRetain(String topicFilter);

}
