package org.example.mqtt.broker;

import java.util.Set;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface Topic {

    TopicFilter topic();

    Set<Session> retainedSession();

    /**
     * add subscriber to the topic
     *
     * @param session subscriber
     * @param qos QoS
     */
    void addSubscriber(Session session, int qos);

    interface TopicFilter {

        String value();

    }

}
