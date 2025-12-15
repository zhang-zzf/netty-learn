package org.github.zzf.mqtt.mqtt.broker;

import java.util.Map;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface Topic {

    String topicFilter();

    /**
     * all the subscribers that subscribe the topic
     *
     * @return all the subscribers
     */
    Map<ServerSession, Integer> subscribers();

    void subscribe(ServerSession session, int qos);

    void unsubscribe(ServerSession session, int qos);

}
