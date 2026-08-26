package org.github.zzf.mqtt.server;

import static java.util.Collections.emptyMap;

import io.micrometer.core.annotation.Timed;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;
import org.github.zzf.mqtt.protocol.server.Authenticator;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.RetainPublishManager;
import org.github.zzf.mqtt.protocol.server.RoutingTable;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.server.Topic;
import org.github.zzf.mqtt.protocol.server.Topic.Shared;
import org.github.zzf.mqtt.protocol.server.TopicBlocker;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultBroker implements Broker {
    public static final String METRIC_NAME = "broker.node.DefaultBroker";
    /**
     * ClientIdentifier -> Session
     */
    // todo 监控 cleanSession = 0 / 1 数量
    final ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();
    final Authenticator authenticator;
    final RoutingTable routingTable;
    final TopicBlocker blockedTopic;
    final RetainPublishManager retainPublishManager;

    public DefaultBroker(Authenticator authenticator,
            RoutingTable routingTable,
            TopicBlocker blockedTopic,
            RetainPublishManager retainPublishManager) {
        this.authenticator = authenticator;
        this.routingTable = routingTable;
        this.blockedTopic = blockedTopic;
        this.retainPublishManager = retainPublishManager;
    }

    @Override
    public CompletionStage<List<Integer>> subscribe(
            ServerSession session,
            Subscribe subscribe) {
        // broker decide
        List<Integer> reasonCodes = decideSubscriptionQos(session, subscribe.subscriptions());
        //
        List<Subscription> granted = subscribe.grantSubscription(reasonCodes);
        return routingTable.subscribe(session.clientIdentifier(), granted)
                .thenApply((unused) -> reasonCodes);
    }

    @Override
    public CompletableFuture<List<Integer>> unsubscribe(
            ServerSession session,
            Unsubscribe packet) {
        List<Integer> reasonCodes = decideUnsubscribeReasonCodes(session, packet);
        return routingTable.unsubscribe(session.clientIdentifier(), packet.subscriptions())
                .thenApply((unused) -> reasonCodes);
    }

    List<Integer> decideUnsubscribeReasonCodes(ServerSession session, Unsubscribe packet) {
        return packet.subscriptions().stream()
                .map(s -> 0x00)
                .toList();
    }

    @Timed(value = METRIC_NAME, histogram = true)
    void doForward(String clientId, Publish packet) {
        for (Topic topic : routingTable.match(packet.topicName())) {
            String topicFilter = topic.topicFilter();
            for (String subscriber : topic.subscribers()) {
                ServerSession session = sessionMap.get(subscriber);
                if (session == null) {// todo metric
                    continue;
                }
                session.forward(clientId, topicFilter, packet);
                if (log.isDebugEnabled()) {
                    log.debug("Publish({}) forward -> tf: {}, client: {}, packet: {}",
                            packet.pId(), topicFilter, session.clientIdentifier(), packet);
                }
            }
        }
    }

    private boolean block(Publish packet) {
        if (blockedTopic == null) {
            return false;
        }
        Topic blocked = this.blockedTopic.match(packet.topicName());
        if (blocked != null) {
            log.info("Broker blocked Publish for matching Topic: Topic: {}, Publish: {}", blocked, packet);
            return true;
        }
        return false;
    }

    @Override
    public ServerSession session(String clientId) {
        return sessionMap.get(clientId);
    }

    @Override
    public byte authenticate(Connect connect) {
        return authenticator.authenticate(connect);
    }

    // todo UT
    // 1. cleanSession = 1 then cleanSession = 1
    // 1. cleanSession = 1 the session should be removed after client disconnect (normally ot not)
    // 1. cleanSession = 0 then cleanSession = 0
    // 1. cleanSession = 0 then cleanSession = 1
    //
    @Override
    public CompletionStage<Void> connect(ServerSession session) {
        String clientIdentifier = session.clientIdentifier();
        sessionMap.put(clientIdentifier, session);
        CompletableFuture<Void> ret = routingTable.subscribe(clientIdentifier, session.subscriptions());
        log.debug("Session({}_{}) connected", clientIdentifier, session.channel().id());
        return ret;
    }

    @Override
    public CompletionStage<Void> disconnect(ServerSession session) {
        sessionMap.remove(session.clientIdentifier(), session);
        // unsubscribe
        CompletableFuture<Void> ret = routingTable.unsubscribe(session.clientIdentifier(), session.subscriptions());
        log.debug("Session({}_{}) disconnected", session.clientIdentifier(), session.channel().id());
        return ret;
    }

    protected List<Integer> decideSubscriptionQos(
            ServerSession session,
            List<Subscription> sub) {
        return sub.stream()
                // change according to the situation
                .map(Subscription::qos)// default to request
                .toList();
    }

    private void retain(Publish publish) {
        if (retainPublishManager == null) {
            log.info("Broker not support retain message");
            return;
        }
        if (!publish.retainFlag()) {
            throw new IllegalArgumentException();
        }
        if (zeroBytesPayload(publish)) {
            log.debug("receive zero bytes payload retain Publish, now remove it: {}", publish);
            // remove the retained message
            retainPublishManager.del(publish);
        }
        else {
            // save the retained message, use a copy of the origin
            retainPublishManager.add(publish.copy());
        }
    }

    @Override
    public void forward(String clientId, Publish packet) {
        // check Blocked TopicFilter
        if (block(packet)) {
            return;
        }
        // retain message
        if (packet.retainFlag()) {
            retain(packet);
        }
        // Broker forward Publish to relative topic after receive a PublishPacket
        doForward(clientId, packet);
    }

    private boolean zeroBytesPayload(Publish publish) {
        return !publish.payload().isReadable();
    }

    @Override
    public void close() {
        // todo 保存 Session 状态，以便下次启动是从 DB 恢复
        if (routingTable != null) {
            try {
                routingTable.close();
            } catch (Exception e) {
                log.error("Close RoutingTable failed: {}", e.getMessage(), e);
            }
        }
        if (blockedTopic != null) {
            try {
                blockedTopic.close();
            } catch (Exception e) {
                log.error("Close TopicBlocker failed: {}", e.getMessage(), e);
            }
        }
        if (retainPublishManager != null) {
            try {
                retainPublishManager.close();
            } catch (Exception e) {
                log.error("Close RetainPublishManager failed: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public CompletableFuture<Map<String, List<Publish>>> retainedPublish(String... topicFilters) {
        if (retainPublishManager != null) {
            return retainPublishManager.match(topicFilters);
        }
        return CompletableFuture.completedFuture(emptyMap());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"broker\":\"")
                .append(this.getClass().getSimpleName()).append("@").append(Integer.toHexString(hashCode()))
                .append('\"').append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static class V50 extends DefaultBroker {

        final RoutingTable sharedRoutingTable;

        public V50(Authenticator authenticator,
                RoutingTable routingTable,
                RoutingTable sharedRoutingTable,
                TopicBlocker blockedTopic,
                RetainPublishManager retainPublishManager) {
            super(authenticator, routingTable, blockedTopic, retainPublishManager);
            this.sharedRoutingTable = sharedRoutingTable;
        }

        @Override
        public CompletionStage<List<Integer>> subscribe(
                ServerSession session,
                Subscribe subscribe) {
            // broker decide
            List<Integer> reasonCodes = decideSubscriptionQos(session, subscribe.subscriptions());
            //
            List<Subscription> subscriptions = new ArrayList<>();
            List<Subscription> sharedSubscriptions = new ArrayList<>();
            for (Subscription sub : subscribe.grantSubscription(reasonCodes)) {
                if (sub instanceof Subscription.V50 v50 && v50.isShared()) {
                    sharedSubscriptions.add(v50);
                }
                else {
                    subscriptions.add(sub);
                }
            }
            String clientId = session.clientIdentifier();
            return routingTable.subscribe(clientId, subscriptions)
                    .thenCompose(unused -> sharedRoutingTable.subscribe(clientId, sharedSubscriptions))
                    .thenApply((unused) -> reasonCodes);
        }

        @Override
        public CompletableFuture<List<Integer>> unsubscribe(
                ServerSession session,
                Unsubscribe packet) {
            List<Integer> reasonCodes = decideUnsubscribeReasonCodes(session, packet);
            List<Subscription> subscriptions = new ArrayList<>();
            List<Subscription> sharedSubscriptions = new ArrayList<>();
            for (Subscription sub : packet.subscriptions()) {
                if (sub instanceof Subscription.V50 v50 && v50.isShared()) {
                    sharedSubscriptions.add(v50);
                }
                else {
                    subscriptions.add(sub);
                }
            }
            String clientId = session.clientIdentifier();
            return routingTable.unsubscribe(clientId, subscriptions)
                    .thenCompose(unused -> sharedRoutingTable.unsubscribe(clientId, sharedSubscriptions))
                    .thenApply((unused) -> reasonCodes);
        }

        @Timed(value = METRIC_NAME, histogram = true)
        void doForward(String clientId, Publish packet) {
            super.doForward(clientId, packet);
            // Shared Subscriptions
            for (Topic topic : sharedRoutingTable.match(packet.topicName())) {
                Topic.SharedTopic sharedTopic = (Topic.SharedTopic) topic;
                for (Shared group : sharedTopic.groups()) {// 订阅组
                    Set<String> subscribers = group.subscribers();
                    // online Session
                    List<ServerSession> onlineSession = subscribers.stream()
                            .sorted()// clientId sort
                            .map(sessionMap::get)
                            .filter(s -> s != null && s.channel().isActive())
                            .toList();
                    int random = ThreadLocalRandom.current().nextInt(onlineSession.size());
                    ServerSession session = onlineSession.get(random);
                    String tf = group.topicFilter();// use origin topicFilter
                    session.forward(clientId, tf, packet);
                    if (log.isDebugEnabled()) {
                        log.debug("Publish({}) forward -> shared.tf: {}, client: {}, packet: {}",
                                packet.pId(), tf, session.clientIdentifier(), packet);
                    }
                }
            }
        }
    }

}
