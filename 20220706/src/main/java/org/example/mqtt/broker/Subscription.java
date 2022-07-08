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
    ServerSession session();

    static Subscription from(String topicFilter, int qos, ServerSession session) {
        return new DefaultSubscription(topicFilter, qos, session);
    }

    /**
     * update qos
     *
     * @param qos the new qos
     */
    Subscription qos(int qos);

    /**
     * @author zhanfeng.zhang
     * @date 2022/06/28
     */
    @RequiredArgsConstructor
    class DefaultSubscription implements Subscription {

        private final String topicFilter;
        private final ServerSession session;
        private int qos;

        public DefaultSubscription(String topicFilter, int qos, ServerSession session) {
            this.topicFilter = topicFilter;
            this.session = session;
            this.qos = qos;
        }

        @Override
        public String topicFilter() {
            return topicFilter;
        }

        @Override
        public int qos() {
            return qos;
        }

        @Override
        public Subscription qos(int qos) {
            this.qos = qos;
            return this;
        }

        @Override
        public ServerSession session() {
            return session;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("{");
            if (topicFilter != null) {
                sb.append("\"topicFilter\":\"").append(topicFilter).append('\"').append(',');
            }
            sb.append("\"qos\":").append(qos).append(',');
            return sb.replace(sb.length() - 1, sb.length(), "}").toString();
        }

    }

}
