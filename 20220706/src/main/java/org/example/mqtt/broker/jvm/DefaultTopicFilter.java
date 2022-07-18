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

    // fuzzy TopicFilter tree
    private final Node root = new Node("*", null);

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
        if (!validate(topicFilter)) {
            throw new IllegalArgumentException("TopicFilter is illegal: " + topicFilter);
        }
        if (!isFuzzyTopic(topicFilter)) {
            preciseTopicFilter.put(topicFilter, EMPTY);
        } else {
            addToFuzzyTopicTree(topicFilter);
        }
    }

    private boolean validate(String topicFilter) {
        if (topicFilter == null) {
            return false;
        }
        int idx;
        if ((idx = topicFilter.indexOf(MULTI_LEVEL_WILDCARD)) != -1 && idx != topicFilter.length() - 1) {
            return false;
        }
        return true;
    }

    private void addToFuzzyTopicTree(String topicFilter) {
        String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
        Node parent = root;
        boolean added = false;
        while (!added) {
            for (int i = 0; i < topicLevels.length; i++) {
                String level = topicLevels[i];
                Node n = lastLevel(i, topicLevels) ? new Node(level, topicFilter) : new Node(level);
                parent = parent.addChild(n);
                if (parent == null) {
                    break;
                }
            }
            added = (parent != null);
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
            dfsRemove(topicFilter.split(LEVEL_SEPARATOR), 0, root);
        }
    }

    private void dfsRemove(String[] topicLevels, int levelIdx, Node parent) {
        if (levelIdx >= topicLevels.length) {
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node n = parent.child(topicLevel);
        if (n == null) {
            return;
        }
        if (lastLevel(levelIdx, topicLevels)) {
            n.topic(null);
        } else {
            dfsRemove(topicLevels, levelIdx + 1, n);
        }
        // try clean child node if needed.
        if (n.canDelete()) {
            parent.removeChild(n);
        }
    }

    private Set<String> fuzzyMatch(String topicName) {
        Set<String> ret = new HashSet<>(4);
        if (topicName == null || topicName.isEmpty()) {
            return ret;
        }
        dfsFuzzyMatch(topicName.split(LEVEL_SEPARATOR), 0, root, ret);
        return ret;
    }

    private void dfsFuzzyMatch(String[] topicLevels, int levelIdx, Node parent, Set<String> ret) {
        if (levelIdx >= topicLevels.length) {
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node n;
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

        private final String level;
        /**
         * null 表示本节点不是 topicFilter
         */
        private volatile String topicFilter;
        /**
         * child Nodes
         */
        private final ConcurrentMap<String, Node> childNodes = new ConcurrentHashMap<>();

        private boolean deleted;

        public Node(String level, String topicFilter) {
            this.level = level;
            this.topicFilter = topicFilter;
        }

        public Node(String level) {
            this(level, null);
        }

        public String topic() {
            return this.topicFilter;
        }

        public Node topic(String topicFilter) {
            this.topicFilter = topicFilter;
            return this;
        }

        public synchronized boolean canDelete() {
            return this.topicFilter == null && this.childNodes.isEmpty();
        }

        public synchronized Node addChild(Node child) {
            if (deleted()) {
                // this node has been deleted
                return null;
            }
            Node nextNode;
            if ((nextNode = childNodes.putIfAbsent(child.level, child)) == null) {
                nextNode = child;
            } else {
                // the child Node is a TopicFilter. must update the exist node in the lock.
                if (child.topic() == null) {
                    nextNode.topic(child.topic());
                }
            }
            return nextNode;
        }

        public synchronized void removeChild(Node node) {
            synchronized (node) {
                if (!node.deleted() && node.canDelete()) {
                    node.deleted = true;
                    // important: must check child node status before remove it from parent.childNodes
                    this.childNodes.remove(node.level, node);
                }
            }
        }

        private boolean deleted() {
            return deleted;
        }

        public Node child(String level) {
            return childNodes.get(level);
        }

    }

}


