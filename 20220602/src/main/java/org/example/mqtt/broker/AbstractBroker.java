package org.example.mqtt.broker;

import org.example.mqtt.model.Connect;

import java.util.List;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class AbstractBroker implements Broker {

    protected abstract List<Topic> topicBy(String topicName);

    protected abstract Topic topicBy(Topic.TopicFilter topicFilter);

    protected abstract int decideSubscriptionQos(Subscription subscription);


    protected abstract AbstractSession initAndBind(Connect packet);

}
