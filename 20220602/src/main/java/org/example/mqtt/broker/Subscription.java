package org.example.mqtt.broker;

import lombok.RequiredArgsConstructor;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public interface Subscription {

    /**
     * topic that the subscription interests.
     *
     * @return topic
     */
    String topicFilter();

    /**
     * maximum Qos
     *
     * @return qos
     */
    int qos();

    /**
     * session that subscribe the topic
     *
     * @return the session
     */
    Session session();

    static Subscription from(String topicFilter, int qos, Session session) {
        return new DefaultSubscription(topicFilter, qos, session);
    }

    /**
     * @author zhanfeng.zhang
     * @date 2022/06/28
     */
    @RequiredArgsConstructor
    class DefaultSubscription implements Subscription {

        private final String topicFilter;
        private final int qos;
        private final Session session;

        @Override
        public String topicFilter() {
            return topicFilter;
        }

        @Override
        public int qos() {
            return qos;
        }

        @Override
        public Session session() {
            return session;
        }

    }

}
