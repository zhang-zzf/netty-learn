package org.example.mqtt.broker;

import lombok.RequiredArgsConstructor;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@RequiredArgsConstructor
public class DefaultSubscription implements Subscription {

    private final Topic.TopicFilter topicFilter;
    private final int qos;
    private final Session session;

    @Override
    public Topic.TopicFilter topic() {
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
