package org.example.mqtt.broker.jvm;

import org.example.mqtt.broker.BrokerState;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Subscribe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class DefaultBrokerState implements BrokerState {

    String LEVEL_SEPARATOR = "/";
    String MULTI_LEVEL_WILDCARD = "#";
    String SINGLE_LEVEL_WILDCARD = "+";

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ConcurrentMap<String, Topic> preciseTopicFilter = new ConcurrentHashMap<>();

    // fuzzy TopicFilter tree
    private final Node root = new Node("*");

    /**
     * ClientIdentifier -> Session
     */
    private final ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();

    @Override
    public ServerSession session(String clientIdentifier) {
        return sessionMap.get(clientIdentifier);
    }

    @Override
    public List<Topic> match(String topicName) {
        List<Topic> match = fuzzyMatch(topicName);
        Topic topic = preciseTopicFilter.get(topicName);
        if (topic != null) {
            match.add(topic);
        }
        return match;
    }

    @Override
    public Future<Void> subscribe(ServerSession session, Subscribe.Subscription subscription) {
        Runnable task = () -> doSubscribe(session, subscription);
        return (Future<Void>) executorService.submit(task);
    }

    private void doSubscribe(ServerSession session, Subscribe.Subscription subscription) {
        String topicFilter = subscription.topicFilter();
        if (!isFuzzyTopic(topicFilter)) {
            Topic topic = preciseTopicFilter.get(topicFilter);
            if (topic == null) {
                topic = new DefaultTopic(topicFilter);
                preciseTopicFilter.put(topicFilter, topic);
            }
            topic.subscribe(session, subscription.qos());
        } else {
            String[] topicLevels = topicFilter.split(LEVEL_SEPARATOR);
            Node parent = root;
            for (int i = 0; i < topicLevels.length; i++) {
                String level = topicLevels[i];
                parent = parent.addChild(new Node(level));
                if (lastLevel(i, topicLevels)) {
                    parent.subscribe(session, subscription.qos(), subscription.topicFilter());
                }
            }
        }
    }

    @Override
    public Future<Void> unsubscribe(ServerSession session, Subscribe.Subscription subscription) {
        Runnable task = () -> doUnsubscribe(session, subscription);
        return (Future<Void>) executorService.submit(task);
    }

    @Override
    public Future<Void> disconnect(ServerSession session) {
        Runnable task = () -> {
            Set<Subscribe.Subscription> subscriptions = session.subscriptions();
            for (Subscribe.Subscription sub : subscriptions) {
                doUnsubscribe(session, sub);
            }
            sessionMap.remove(session.clientIdentifier(), session);
        };
        return (Future<Void>) executorService.submit(task);
    }

    @Override
    public Future<ServerSession> connect(ServerSession session) {
        Callable task = () -> {
            ServerSession exist = sessionMap.putIfAbsent(session.clientIdentifier(), session);
            if (exist != null) {
                return exist;
            }
            Set<Subscribe.Subscription> subscriptions = session.subscriptions();
            for (Subscribe.Subscription sub : subscriptions) {
                doSubscribe(session, sub);
            }
            return null;
        };
        return executorService.submit(task);

    }

    private void doUnsubscribe(ServerSession session, Subscribe.Subscription subscription) {
        String topicFilter = subscription.topicFilter();
        if (!isFuzzyTopic(topicFilter)) {
            removePreciseTopic(session, subscription, topicFilter);
        } else {
            dfsRemoveFuzzyTopic(session, subscription, topicFilter.split(LEVEL_SEPARATOR), 0, root);
        }
    }

    private void removePreciseTopic(ServerSession session, Subscribe.Subscription subscription, String topicFilter) {
        Topic topic = preciseTopicFilter.get(topicFilter);
        if (topic == null) {
            return;
        }
        topic.unsubscribe(session, subscription.qos());
        if (topic.subscribers().isEmpty()) {
            preciseTopicFilter.remove(topicFilter, topic);
        }
    }

    private void dfsRemoveFuzzyTopic(ServerSession session, Subscribe.Subscription subscription, String[] topicLevels,
                                     int levelIdx, Node parent) {
        if (levelIdx >= topicLevels.length) {
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        Node n = parent.child(topicLevel);
        if (n == null) {
            return;
        }
        if (lastLevel(levelIdx, topicLevels)) {
            n.unsubscribe(session, subscription);
        } else {
            dfsRemoveFuzzyTopic(session, subscription, topicLevels, levelIdx + 1, n);
        }
        // try clean child node if needed.
        if (n.canDelete()) {
            parent.removeChild(n);
        }
    }

    private boolean isFuzzyTopic(String topicFilter) {
        return topicFilter.contains(MULTI_LEVEL_WILDCARD) || topicFilter.contains(SINGLE_LEVEL_WILDCARD);
    }

    private List<Topic> fuzzyMatch(String topicName) {
        List<Topic> ret = new ArrayList<>(2);
        if (topicName == null || topicName.isEmpty()) {
            return ret;
        }
        dfsFuzzyMatch(topicName.split(LEVEL_SEPARATOR), 0, root, ret);
        return ret;
    }

    private void dfsFuzzyMatch(String[] topicLevels, int levelIdx, Node parent, List<Topic> ret) {
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

    private void addNode(List<Topic> ret, Node n) {
        if (n.topic == null) {
            return;
        }
        ret.add(n.topic);
    }

    private boolean lastLevel(int level, String[] levelArray) {
        return level == levelArray.length - 1;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
    }

    public static class Node {

        private final String level;
        /**
         * null 表示本节点不是 topicFilter
         */
        private volatile Topic topic;
        /**
         * child Nodes
         */
        private final ConcurrentMap<String, Node> childNodes = new ConcurrentHashMap<>();

        public Node(String level) {
            this.level = level;
        }

        public boolean canDelete() {
            boolean notTopic = (this.topic == null || this.topic.subscribers().isEmpty());
            boolean noChild = this.childNodes.isEmpty();
            return notTopic && noChild;
        }

        public Node addChild(Node child) {
            Node nextNode;
            if ((nextNode = childNodes.putIfAbsent(child.level, child)) == null) {
                nextNode = child;
            }
            return nextNode;
        }

        public void removeChild(Node node) {
            this.childNodes.remove(node.level, node);
        }

        public Node child(String level) {
            return childNodes.get(level);
        }

        public void subscribe(ServerSession session, int qos, String topicFilter) {
            if (this.topic == null) {
                this.topic = new DefaultTopic(topicFilter);
            }
            this.topic.subscribe(session, qos);
        }

        public void unsubscribe(ServerSession session, Subscribe.Subscription subscription) {
            if (this.topic == null) {
                return;
            }
            this.topic.unsubscribe(session, subscription.qos());
        }

    }

}
