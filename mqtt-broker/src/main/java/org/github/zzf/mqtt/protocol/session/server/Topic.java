package org.github.zzf.mqtt.protocol.session.server;

import java.util.Iterator;
import java.util.List;

public interface Topic {

    String topicFilter();

    /**
     * all the subscribers that subscribe the topic
     */
    List<Subscriber> subscribers();

    interface Subscriber {
        String clientId();

        int qos();
    }

    // void subscribe(ServerSession session, int qos);
    //
    // void unsubscribe(ServerSession session, int qos);

}
