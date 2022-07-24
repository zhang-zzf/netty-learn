package org.example.mqtt.broker;

import java.util.Set;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/13
 */
public interface TopicFilter {

    String LEVEL_SEPARATOR = "/";
    String MULTI_LEVEL_WILDCARD = "#";
    String SINGLE_LEVEL_WILDCARD = "+";


    /**
     * match
     *
     * @param topicName topicName
     * @return the TopicFilter that match the topicName
     */
    Set<String> match(String topicName);

    /**
     * add TopicFilter
     *
     * @param topicFilter TopicFilter
     */
    void add(String topicFilter);

    /**
     * remove TopicFilter
     *
     * @param topicFilter TopicFilter
     */
    void remove(String topicFilter);

}
