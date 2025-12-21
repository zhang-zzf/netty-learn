package org.github.zzf.mqtt.protocol.session.server;

import java.util.Map;

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
