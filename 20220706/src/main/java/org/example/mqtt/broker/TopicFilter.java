package org.example.mqtt.broker;

import java.util.Set;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/13
 */
public interface TopicFilter {

    Set<String> match(String topicName);

    void add(String topicFilter);

    void remove(String topicFilter);

}
