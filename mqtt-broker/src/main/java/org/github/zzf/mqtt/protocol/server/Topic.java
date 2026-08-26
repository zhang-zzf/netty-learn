package org.github.zzf.mqtt.protocol.server;

import java.util.Set;

public interface Topic {

    String topicFilter();

    /**
     * all the subscribers (clientId) that subscribe the topic
     */
    Set<String> subscribers();

    interface SharedTopic extends Topic {
        Set<Shared> groups();
    }

    interface Shared {

        // the full topicFilter in Subscribe
        String topicFilter();

        String group();

        Set<String> subscribers();
    }

}
