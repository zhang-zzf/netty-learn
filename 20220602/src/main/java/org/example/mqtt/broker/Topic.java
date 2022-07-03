package org.example.mqtt.broker;

import java.util.Map;
import java.util.Set;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface Topic extends AutoCloseable {

    String topicFilter();

    Set<Session> retainedSession();

    /**
     * add subscriber to the topic
     *
     * @param session subscriber
     * @param qos QoS
     */
    void addSubscriber(ServerSession session, int qos);

    /**
     * remove a subscriber
     *
     * @param session Session
     */
    void removeSubscriber(Session session);

    /**
     * all the subscribers that subscribe the topic
     *
     * @return all the subscribers
     */
    Map<ServerSession, Integer> subscribers();

    boolean isEmpty();


}
