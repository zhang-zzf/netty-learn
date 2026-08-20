package org.github.zzf.mqtt.server;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import io.micrometer.core.annotation.Timed;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.server.Authenticator;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.RetainPublishManager;
import org.github.zzf.mqtt.protocol.server.RoutingTable;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.server.Topic;
import org.github.zzf.mqtt.protocol.server.Topic.Subscriber;
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
    public List<Subscribe.Subscription> subscribe(
            ServerSession session,
            Collection<Subscription> subscriptions) {
        if (subscriptions == null) {
            return emptyList();
        }
        List<Subscription> permitted = subscriptions.stream()
                .map(sub -> decideSubscriptionQos(session, sub))
                .collect(toList());
        // sync
        routingTable.subscribe(session.clientIdentifier(), permitted).join();
        if (retainPublishManager != null) {
            //
            String[] tfs = permitted.stream().map(Subscription::topicFilter).toArray(String[]::new);
            // async
            retainPublishManager.match(tfs).thenAccept(publishPackets -> {
                // 移交给 session 绑定的线程，延迟发送
                session.channel().eventLoop().submit(() -> publishPackets.forEach(session::write));
            });
        }
        return permitted;
    }

    @Override
    public void unsubscribe(ServerSession session, Collection<Subscription> subscriptions) {
        // sync
        routingTable.unsubscribe(session.clientIdentifier(), subscriptions).join();
    }

    @Timed(value = METRIC_NAME, histogram = true)
    private int doForward(Publish packet) {
        int times = 0;
        for (Topic topic : routingTable.match(packet.topicName())) {
            for (Subscriber subscriber : topic.subscribers()) {
                ServerSession session = sessionMap.get(subscriber.clientId());
                if (session == null) {// todo metric
                    continue;
                }
                // qos to use for the Publish packet
                int qos = qoS(packet.qos(), subscriber.qos());
                session.write(topic.topicFilter(), qos, packet);
                if (log.isDebugEnabled()) {
                    log.debug("Publish({}) forward -> tf: {}, client: {}, packet: {}",
                            packet.pId(), topic.topicFilter(), session.clientIdentifier(), packet);
                }
                times += 1;
            }
        }
        return times;
    }

    private int qoS(int packetQos, int tfQos) {
        return Math.min(packetQos, tfQos);
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

    protected Subscribe.Subscription decideSubscriptionQos(
            ServerSession session,
            Subscribe.Subscription sub) {
        // todo decide qos
        int qos = sub.qos();
        return new Subscribe.Subscription(sub.topicFilter(), (byte) qos);
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
            // use a copy of the origin
            Publish packet = Publish.outgoing(publish.retainFlag(), (byte) publish.qos(), publish.dup(),
                    publish.topicName(), (short) 0,
                    publish.payload().copy());
            // save the retained message
            retainPublishManager.add(packet);
        }
    }

    @Override
    public int forward(Publish packet) {
        // check Blocked TopicFilter
        if (block(packet)) {
            return 0;
        }
        // retain message
        if (packet.retainFlag()) {
            retain(packet);
        }
        // Broker forward Publish to relative topic after receive a PublishPacket
        return doForward(packet);
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
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"broker\":\"")
                .append(this.getClass().getSimpleName()).append("@").append(Integer.toHexString(hashCode()))
                .append('\"').append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    public static class V50 extends DefaultBroker {

        public V50(Authenticator authenticator,
                RoutingTable routingTable,
                TopicBlocker blockedTopic,
                RetainPublishManager retainPublishManager) {
            super(authenticator, routingTable, blockedTopic, retainPublishManager);
        }

    }
}
