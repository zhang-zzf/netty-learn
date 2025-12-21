package org.github.zzf.mqtt.mqtt.broker.node;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.github.zzf.mqtt.protocol.model.Publish.NO_PACKET_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.Publish.needAck;

import io.micrometer.core.annotation.Timed;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.micrometer.utils.MetricUtil;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.session.server.Authenticator;
import org.github.zzf.mqtt.protocol.session.server.Broker;
import org.github.zzf.mqtt.protocol.session.server.ServerSession;
import org.github.zzf.mqtt.protocol.session.server.Topic;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultBroker implements Broker {

    private final Authenticator authenticator;

    private final DefaultBrokerState brokerState = new DefaultBrokerState();

    private volatile TopicFilterTree blockedTopicFilter;

    public DefaultBroker(Authenticator authenticator) {
        this.authenticator = authenticator;
        this.self = this;
        initBlockedTopicFilter();
    }

    private void initBlockedTopicFilter() {
        String tfConfig = System.getProperty("mqtt.server.block.tf", "+/server/#");
        Set<String> blockedTf = Arrays.stream(tfConfig.split(",")).collect(toSet());
        blockedTopicFilter = TopicFilterTree.from(blockedTf);
    }

    @Override
    public List<Subscribe.Subscription> subscribe(ServerSession session, Subscribe subscribe) {
        List<Subscribe.Subscription> subscriptions = subscribe.subscriptions();
        List<Subscribe.Subscription> permittedSub = new ArrayList<>(subscriptions.size());
        for (Subscribe.Subscription sub : subscriptions) {
            Subscribe.Subscription permitted = decideSubscriptionQos(session, sub);
            brokerState.subscribe(session, permitted);
            permittedSub.add(permitted);
        }
        return permittedSub;
    }

    @Override
    public void unsubscribe(ServerSession session, Unsubscribe packet) {
        for (Subscribe.Subscription subscription : packet.subscriptions()) {
            brokerState.unsubscribe(session, subscription);
        }
    }

    @Timed(value = METRIC_NAME, histogram = true)
    @Override
    public int forward(Publish packet) {
        int times = 0;
        for (Topic topic : brokerState.match(packet.topicName())) {
            for (Map.Entry<ServerSession, Integer> e : topic.subscribers().entrySet()) {
                ServerSession session = e.getKey();
                int qos = qoS(packet.qos(), e.getValue());
                // use a shadow copy of the origin Publish
                Publish outgoing = Publish.outgoing(false /* must set retain to false before forward the PublishPacket */,
                    qos, false, topic.topicFilter(), packetIdentifier(session, qos), packet.payload());
                if (log.isDebugEnabled()) {
                    log.debug("Publish({}) forward -> tf: {}, client: {}, packet: {}", packet.pId(), topic.topicFilter(), session.clientIdentifier(), outgoing);
                }
                session.send(outgoing);
                times += 1;
            }
        }
        return times;
    }

    public static short packetIdentifier(ServerSession session, int qos) {
        return needAck(qos) ? session.nextPacketIdentifier() : NO_PACKET_IDENTIFIER;
    }

    public static int qoS(int packetQos, int tfQos) {
        return Math.min(packetQos, tfQos);
    }

    @Override
    @Nullable
    public ServerSession session(String clientIdentifier) {
        return brokerState.session(clientIdentifier);
    }

    @Override
    public Map<String, ServerSession> sessionMap() {
        return brokerState.session();
    }

    @Override
    public boolean block(Publish packet) {
        return !blockedTopicFilter.match(packet.topicName()).isEmpty();
    }

    @Override
    public ServerSession onConnect(Connect connect, Channel channel) {
        log.debug("Server receive Connect from client({}) -> {}", connect.clientIdentifier(), connect);
        // The Server MUST respond to the CONNECT Packet
        // with a CONNACK return code 0x01 (unacceptable protocol level) and then
        // disconnect the Client if the Protocol Level is not supported by the Server
        if (!supportProtocolLevel().contains(connect.protocolLevel())) {
            log.error("Server not support protocol level, now send ConnAck and close channel to client({})", connect.clientIdentifier());
            channel.writeAndFlush(ConnAck.notSupportProtocolLevel()).channel().close();
        }
        // authenticate
        int authenticate = authenticator.authenticate(connect);
        if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
            log.error("Server authenticate Connect from client({}) failed, now send ConnAck and close channel -> {}", connect.clientIdentifier(), authenticate);
            channel.writeAndFlush(new ConnAck(authenticate)).channel().close();
        }

        return null;
    }

    @SneakyThrows
    @Override
    public void detachSession(ServerSession session, boolean force) {
        if (session == null) {
            return;
        }
        // close channel
        // todo
        // if (session.isActive()) {
        //     session.close();
        // }
        if (session.cleanSession() || force) {
            String cId = session.clientIdentifier();
            log.debug("Broker({}) now try to remove Session({})", this, cId);
            // broker clear session state
            brokerState.remove(session).get();
            log.debug("Broker removed Session({})", cId);
        }
    }

    protected Subscribe.Subscription decideSubscriptionQos(ServerSession session, Subscribe.Subscription sub) {
        // todo decide qos
        int qos = sub.qos();
        return new Subscribe.Subscription(sub.topicFilter(), qos);
    }

    @Override
    public Optional<Topic> topic(String topicFilter) {
        return brokerState.topic(topicFilter);
    }

    @Override
    public Set<Integer> supportProtocolLevel() {
        return new HashSet<>(Arrays.asList(4));
    }

    private void retain(Publish publish) {
        if (!publish.retainFlag()) {
            throw new IllegalArgumentException();
        }
        // use a copy of the origin
        Publish packet = Publish.outgoing(publish.retainFlag(), (byte) publish.qos(),
            publish.dup(), publish.topicName(), (short) 0, publish.payload().copy());
        if (zeroBytesPayload(packet)) {
            log.debug("receive zero bytes payload retain Publish, now remove it: {}", packet);
            // remove the retained message
            brokerState.removeRetain(packet);
        }
        else {
            // save the retained message
            brokerState.saveRetain(packet);
        }
    }

    @Override
    public List<Publish> retainMatch(String topicFilter) {
        return brokerState.matchRetain(topicFilter);
    }

    public static final String METRIC_NAME = "broker.node.DefaultBroker";

    @Override
    public void handlePublish(Publish packet) {
        // retain message
        if (packet.retainFlag()) {
            retain(packet);
        }
        // Broker forward Publish to relative topic after receive a PublishPacket
        self.forward(packet);
    }

    @Override
    @SneakyThrows
    public boolean attachSession(ServerSession session) {
        // sync wait
        ServerSession previous = brokerState.add(session).get();
        if (previous != null) {
            // todo
            // previous.close();
        }
        String cId = session.clientIdentifier();
        if (session.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session, Broker now has Session: {}", cId, previous);
            if (previous != null && !previous.cleanSession()) {
                detachSession(previous, true);
                log.debug("Client({})'s old Session was removed", cId);
            }
            log.debug("Client({}) connected to Broker with Session: {}", cId, session);
        }
        else {
            log.debug("Client({}) need a (cleanSession=0) Session, Broker has Session: {}", cId, previous);
            // CleanSession MUST NOT be reused in any subsequent Session
            // if previous.cleanSession is true, then broker should use the new created Session
            if (previous == null || previous.cleanSession()) {
                log.debug("Client({}) need a (cleanSession=0) Session, new Session created", cId);
            }
            else {
                log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", cId, previous);
                session.migrate(previous);
                log.debug("Client({}) need a (cleanSession=0) Session, exist Session migrated: {}", cId, session);
                return true;
            }
        }
        return false;
    }

    private boolean zeroBytesPayload(Publish publish) {
        return !publish.payload().isReadable();
    }

    @Override
    public void close() throws Exception {
        // Server was started by the Broker
        // So it should not be closed by the Broker
        brokerState.close();
    }

    /**
     * use for Aop proxy
     */
    private Broker self;

    public void setSelf(Broker self) {
        this.self = self;
    }

    @Slf4j
    public static class DefaultBrokerState {

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
         * retain Publish topicName <-> Publish
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
            }
            else {
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
            }
            else {
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
            }
            else {
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
            }
            else {
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

    /**
     * TopicFilter 树
     */
    public static class TopicFilterTree {

        static String LEVEL_SEPARATOR = "/";
        static String MULTI_LEVEL_WILDCARD = "#";
        static String SINGLE_LEVEL_WILDCARD = "+";

        private TopicFilterTree() {

        }

        public static TopicFilterTree from(Set<String> topicFilters) {
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"broker\":\"")
            .append(this.getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()))
            .append('\"').append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }
}
