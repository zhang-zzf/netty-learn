package org.example.mqtt.broker.node;

import lombok.extern.slf4j.Slf4j;
import org.example.micrometer.utils.MetricUtil;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.*;
import java.util.concurrent.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@Slf4j
public class DefaultBrokerState {

    static String LEVEL_SEPARATOR = "/";
    static String MULTI_LEVEL_WILDCARD = "#";
    static String SINGLE_LEVEL_WILDCARD = "+";

    private final ExecutorService executorService = new ThreadPoolExecutor(1, 1,
            60, TimeUnit.SECONDS,
            // 使用无界队列
            new LinkedBlockingDeque<>(),
            (r) -> new Thread(r, "broker-state"),
            new ThreadPoolExecutor.AbortPolicy()
    );
    private final ConcurrentMap<String, Topic> preciseTopicFilter = new ConcurrentHashMap<>();

    // fuzzy TopicFilter tree
    private final Node root = new Node("*");

    /**
     * ClientIdentifier -> Session
     */
    private final ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();

    /**
     * retain Publish
     * topicName <-> Publish
     */
    private final ConcurrentMap<String, Publish> retainedPublish = new ConcurrentHashMap<>();

    public DefaultBrokerState() {
        initMetrics();
    }

    private void initMetrics() {
        MetricUtil.gauge("broker.cluster.node.broker.clients", sessionMap);
    }


    public ServerSession session(String clientIdentifier) {
        return sessionMap.get(clientIdentifier);
    }

    public Map<String, ServerSession> session() {
        return Collections.unmodifiableMap(sessionMap);
    }

    public List<Topic> match(String topicName) {
        List<Topic> match = fuzzyMatch(topicName);
        Topic topic = preciseTopicFilter.get(topicName);
        if (topic != null) {
            match.add(topic);
        }
        return match;
    }

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

    public Future<?> unsubscribe(ServerSession session, Subscribe.Subscription subscription) {
        Runnable task = () -> doUnsubscribe(session, subscription);
        return executorService.submit(task);
    }

    public Future<?> remove(ServerSession session) {
        Runnable task = () -> {
            Set<Subscribe.Subscription> subscriptions = session.subscriptions();
            for (Subscribe.Subscription sub : subscriptions) {
                doUnsubscribe(session, sub);
            }
            sessionMap.remove(session.clientIdentifier(), session);
        };
        return executorService.submit(task);
    }

    public Future<ServerSession> add(ServerSession session) {
        Callable<ServerSession> task = () -> {
            ServerSession previous = sessionMap.put(session.clientIdentifier(), session);
            Set<Subscribe.Subscription> subscriptions = session.subscriptions();
            for (Subscribe.Subscription sub : subscriptions) {
                doSubscribe(session, sub);
            }
            return previous;
        };
        return executorService.submit(task);
    }

    public void removeRetain(Publish packet) {
        retainedPublish.remove(packet.topicName());
    }

    public void saveRetain(Publish packet) {
        retainedPublish.put(packet.topicName(), packet);
    }

    public List<Publish> matchRetain(String topicFilter) {
        if (!isFuzzyTopic(topicFilter)) {
            Publish publish = retainedPublish.get(topicFilter);
            return publish == null ? emptyList() : singletonList(publish);
        }
        // O(n) 全量匹配
        return retainedPublish.entrySet().stream()
                .filter(e -> topicNameMatchTopicFilter(e.getKey(), topicFilter))
                .map(Map.Entry::getValue).collect(toList());
    }

    public Optional<Topic> topic(String topicFilter) {
        if (!isFuzzyTopic(topicFilter)) {
            return Optional.ofNullable(preciseTopicFilter.get(topicFilter));
        } else {
            Node idx = root;
            for (String l : topicFilter.split(LEVEL_SEPARATOR)) {
                idx = idx.child(l);
                if (idx == null) {
                    break;
                }
            }
            return Optional.ofNullable(idx).map(n -> n.topic);
        }
    }

    static boolean topicNameMatchTopicFilter(String topicName, String topicFilter) {
        String[] names = topicName.split("/");
        String[] filters = topicFilter.split("/");
        for (int i = 0; i < Math.min(names.length, filters.length); i++) {
            if (MULTI_LEVEL_WILDCARD.equals(filters[i])) {
                return true;
            }
            if (SINGLE_LEVEL_WILDCARD.equals(filters[i])) {
                continue;
            }
            if (!filters[i].equals(names[i])) {
                return false;
            }
        }
        //
        if (names.length == filters.length) {
            return true;
        }
        if (filters.length == names.length + 1
                && MULTI_LEVEL_WILDCARD.equals(filters[filters.length - 1])) {
            return true;
        }
        return false;
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
