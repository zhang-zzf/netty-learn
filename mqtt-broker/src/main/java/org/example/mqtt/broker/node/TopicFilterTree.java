package org.example.mqtt.broker.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TopicFilter 树
 */
public class TopicFilterTree {

    static String LEVEL_SEPARATOR = "/";
    static String MULTI_LEVEL_WILDCARD = "#";
    static String SINGLE_LEVEL_WILDCARD = "+";

    private TopicFilterTree() {

    }

    public static final TopicFilterTree from(Set<String> topicFilters) {
        TopicFilterTree tft = new TopicFilterTree();
        for (String topicFilter : topicFilters) {
            tft.addTopicFilter(topicFilter);
        }
        return tft;
    }

    // fuzzy TopicFilter tree
    private final Node root = new Node("*");

    private void addTopicFilter(String topicFilter) {
        String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
        Node parent = root;
        for (int i = 0; i < topicLevels.length; i++) {
            String level = topicLevels[i];
            parent = parent.addChild(new Node(level));
            if (lastLevel(i, topicLevels)) {
                parent.topic = topicFilter;
            }
        }
    }

    public List<String> match(String topicName) {
        List<String> ret = new ArrayList<>(2);
        if (topicName == null || topicName.isEmpty()) {
            return ret;
        }
        dfsFuzzyMatch(topicName.split(LEVEL_SEPARATOR), 0, root, ret);
        return ret;
    }

    private void addNode(List<String> ret, Node n) {
        if (n.topic == null) {
            return;
        }
        ret.add(n.topic);
    }

    private void dfsFuzzyMatch(String[] topicLevels, int levelIdx, Node parent, List<String> ret) {
        Node n;
        if (levelIdx == topicLevels.length) {
            if ((n = parent.child(MULTI_LEVEL_WILDCARD)) != null) {
                addNode(ret, n);
            }
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        if ((n = parent.child(topicLevel)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, n);
            }
            dfsFuzzyMatch(topicLevels, levelIdx + 1, n, ret);
        }
        if ((n = parent.child(MULTI_LEVEL_WILDCARD)) != null) {
            addNode(ret, n);
        }
        if ((n = parent.child(SINGLE_LEVEL_WILDCARD)) != null) {
            if (lastLevel(levelIdx, topicLevels)) {
                addNode(ret, n);
            }
            dfsFuzzyMatch(topicLevels, levelIdx + 1, n, ret);
        }
    }

    private boolean lastLevel(int level, String[] levelArray) {
        return level == levelArray.length - 1;
    }

    public static class Node {

        private String level;
        /**
         * null 表示本节点不是 topicFilter
         */
        private String topic;
        /**
         * child Nodes
         */
        private final ConcurrentMap<String, Node> childNodes = new ConcurrentHashMap<>(2);

        public Node(String level) {
            this(level, null);
        }

        public Node(String level, String topic) {
            this.level = level;
            this.topic = topic;
        }

        public Node addChild(Node child) {
            Node nextNode;
            if ((nextNode = childNodes.putIfAbsent(child.level, child)) == null) {
                nextNode = child;
            }
            return nextNode;
        }

        public Node child(String level) {
            return childNodes.get(level);
        }

    }

}
