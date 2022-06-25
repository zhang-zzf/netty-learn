package org.example.codec.mqtt;

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
    String topic();

    /**
     * maximum Qos
     *
     * @return qos
     */
    int qos();

    /**
     * session that subscribe the topic
     */
    Session session();

}
