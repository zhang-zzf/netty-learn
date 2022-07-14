package org.example.mqtt.broker.jvm;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.TopicFilter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/13
 */
@Slf4j
public class DefaultTopicFilter implements TopicFilter {

    public static final String LEVEL_SEPARATOR = "/";
    public static final String MULTI_LEVEL_WILDCARD = "#";
    public static final String SINGLE_LEVEL_WILDCARD = "+";

    private final Object EMPTY = new Object();
    private final ConcurrentMap<String, Object> preciseTopicFilter = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Node> fuzzyTree = new ConcurrentHashMap<>();

    @Override
    public Set<String> match(String topicName) {
        Set<String> fuzzyMatch = fuzzyMatch(topicName);
        if (preciseMatch(topicName)) {
            fuzzyMatch.add(topicName);
        }
        return fuzzyMatch;
    }

    @Override
    public void add(String topicFilter) {
        if (topicFilter == null) {
            throw new NullPointerException();
        }
        if (!isFuzzyTopic(topicFilter)) {
            preciseTopicFilter.put(topicFilter, EMPTY);
        } else {
            addToFuzzyTopicTree(topicFilter);
        }
    }

    private void addToFuzzyTopicTree(String topicFilter) {
        String[] topicLevel = topicFilter.split(LEVEL_SEPARATOR);
        ConcurrentMap<String, Node> curLevel = fuzzyTree;
        for (int i = 0; i < topicLevel.length; i++) {
            Node n = lastLevel(i, topicLevel) ? new Node(topicFilter) : new Node();
            Node previous = curLevel.putIfAbsent(topicLevel[i], n);
            if (previous != null) {
                previous.topic(topicFilter);
                curLevel = previous.childLevel();
            } else {
                curLevel = n.childLevel();
            }
        }
    }

    private boolean isFuzzyTopic(String topicFilter) {
        return topicFilter.contains(MULTI_LEVEL_WILDCARD) || topicFilter.contains(SINGLE_LEVEL_WILDCARD);
    }

    @Override
    public void remove(String topicFilter) {
        if (topicFilter == null) {
            throw new NullPointerException();
        }
        if (!isFuzzyTopic(topicFilter)) {
            preciseTopicFilter.remove(topicFilter);
        } else {
            removeFromFuzzyTopicTree(topicFilter);
        }
    }

    private void removeFromFuzzyTopicTree(String topicFilter) {
        String[] topicLevel = topicFilter.split(LEVEL_SEPARATOR);
        ConcurrentMap<String, Node> curLevel = this.fuzzyTree;
        for (int i = 0; i < topicLevel.length; i++) {
            Node n = curLevel.get(topicLevel[i]);
            if (n == null) {
                return;
            }
            if (lastLevel(i, topicLevel)) {
                // todo Not ThreadSafe
                n.topic(null);
            }
        }
    }

    private Set<String> fuzzyMatch(String topicName) {
        Set<String> ret = new HashSet<>(4);
        if (topicName == null || topicName.isEmpty()) {
            return ret;
        }
        dfsFuzzyMatch(topicName.split(LEVEL_SEPARATOR), 0, fuzzyTree, ret);
        return ret;
    }

    private void dfsFuzzyMatch(String[] topicLevels, int levelIdx, ConcurrentMap<String, Node> nodes, Set<String> ret) {
        if (levelIdx >= topicLevels.length) {
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node levelNode;
        if ((levelNode = nodes.get(topicLevel)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, levelNode);
            }
            dfsFuzzyMatch(topicLevels, levelIdx + 1, levelNode.childLevel(), ret);
        }
        if ((levelNode = nodes.get(MULTI_LEVEL_WILDCARD)) != null) {
            addNode(ret, levelNode);
        }
        if ((levelNode = nodes.get(SINGLE_LEVEL_WILDCARD)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, levelNode);
            }
            dfsFuzzyMatch(topicLevels, levelIdx + 1, levelNode.childLevel(), ret);
        }
    }

    private void addNode(Set<String> ret, Node n) {
        if (n.topic() != null) {
            ret.add(n.topic());
        }
    }

    private boolean lastLevel(int level, String[] levelArray) {
        return level == levelArray.length - 1;
    }

    private boolean preciseMatch(String topicName) {
        return preciseTopicFilter.containsKey(topicName);
    }

    public static class Node {

        /**
         * null 表示本节点不是 topicFilter
         */
        private String topic;
        final ConcurrentMap<String, Node> childLevel = new ConcurrentHashMap<>();

        public Node(String topic) {
            this.topic = topic;
        }

        public Node() {

        }

        public ConcurrentMap<String, Node> childLevel() {
            return this.childLevel;
        }

        public String topic() {
            return this.topic;
        }

        public Node topic(String topicFilter) {
            this.topic = topicFilter;
            return this;
        }

    }

}


