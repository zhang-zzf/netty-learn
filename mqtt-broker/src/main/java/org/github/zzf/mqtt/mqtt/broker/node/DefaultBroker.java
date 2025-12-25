package org.github.zzf.mqtt.mqtt.broker.node;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.github.zzf.mqtt.protocol.model.Publish.NO_PACKET_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.Publish.needAck;

import io.micrometer.core.annotation.Timed;
import io.netty.channel.Channel;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.Connect.AuthenticationException;
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
import org.github.zzf.mqtt.server.RoutingTableImpl;
import org.github.zzf.mqtt.server.TopicBlockerImpl;
import org.github.zzf.mqtt.server.TopicTreeRetain;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultBroker implements Broker {

    final Authenticator authenticator;

    final RoutingTable routingTable = new RoutingTableImpl();
    /**
     * ClientIdentifier -> Session
     */
    // todo 监控 cleanSession = 0 / 1 数量
    final ConcurrentMap<String, ServerSession> sessionMap = new ConcurrentHashMap<>();

    final TopicBlocker blockedTopic = TopicBlockerImpl.DEFAULT;

    final RetainPublishManager retainPublishManager = TopicTreeRetain.DEFAULT;

    public DefaultBroker(Authenticator authenticator) {
        this.authenticator = authenticator;
        this.self = this;
    }

    @Override
    public List<Subscribe.Subscription> subscribe(ServerSession session, Collection<Subscription> subscriptions) {
        if (subscriptions == null) {
            return emptyList();
        }
        List<Subscription> permitted = subscriptions.stream()
            .map(sub -> decideSubscriptionQos(session, sub))
            .collect(toList());
        // sync
        routingTable.subscribe(session.clientIdentifier(), permitted).join();
        //
        String[] tfs = permitted.stream().map(Subscription::topicFilter).toArray(String[]::new);
        // async
        retainPublishManager.match(tfs).thenAccept(publishPackets -> {
            // 移交给 session 绑定的线程，延迟发送
            session.channel().eventLoop().submit(() -> {
                for (Publish publish : publishPackets) {
                    session.send(publish);
                }
            });
        });
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
                if (session == null) {
                    // todo metric
                    continue;
                }
                int qos = qoS(packet.qos(), subscriber.qos());
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

    private boolean block(Publish packet) {
        Topic blocked = this.blockedTopic.match(packet.topicName());
        if (blocked != null) {
            log.info("Broker blocked Publish for matching Topic: Topic: {}, Publish: {}", blocked, packet);
            return true;
        }
        return false;
    }

    public static short packetIdentifier(ServerSession session, int qos) {
        return needAck(qos) ? session.nextPacketIdentifier() : NO_PACKET_IDENTIFIER;
    }

    public static int qoS(int packetQos, int tfQos) {
        return Math.min(packetQos, tfQos);
    }

    // todo UT
    // 1. cleanSession = 1 then cleanSession = 1
    // 1. cleanSession = 1 the session should be removed after client disconnect (normally ot not)
    // 1. cleanSession = 0 then cleanSession = 0
    // 1. cleanSession = 0 then cleanSession = 1
    //
    @Override
    public ServerSession onConnect(Connect connect, Channel channel) {
        log.debug("Server receive Connect from client({}) -> {}", connect.clientIdentifier(), connect);
        // The Server MUST respond to the CONNECT Packet
        // with a CONNACK return code 0x01 (unacceptable protocol level) and then
        // disconnect the Client if the Protocol Level is not supported by the Server
        if (!supportProtocolLevel().contains(connect.protocolLevel())) {
            throw new UnsupportedOperationException();
        }
        // authenticate
        int authenticate = authenticator.authenticate(connect);
        if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
            throw new AuthenticationException(authenticate);
        }
        //
        String clientId = connect.clientIdentifier();
        ServerSession previous = sessionMap.get(clientId);
        if (previous != null) {
            previous.channel().close();
            sessionMap.remove(clientId, previous);
        }
        DefaultServerSession session;
        // todo channelClose remove session and subscriptions
        if (connect.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session", clientId);
            session = new DefaultServerSession(connect, channel, this);
            log.debug("Client({}) connected to Broker with Session: {}", clientId, session);
        }
        else {
            log.debug("Client({}) need a (cleanSession=0) Session", clientId);
            // CleanSession MUST NOT be reused in any subsequent Session
            // if previous.cleanSession is true, then broker should use the new created Session
            if (previous == null || previous.cleanSession()) {
                session = new DefaultServerSession(connect, channel, this);
                log.debug("Client({}) need a (cleanSession=0) Session, new Session created", clientId);
            }
            else {
                log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", clientId, previous);
                session = new DefaultServerSession(connect, channel, this, previous);
            }
        }
        // attach session
        sessionMap.put(session.clientIdentifier(), session);
        return session;
    }

    protected Subscribe.Subscription decideSubscriptionQos(ServerSession session, Subscribe.Subscription sub) {
        // todo decide qos
        int qos = sub.qos();
        return new Subscribe.Subscription(sub.topicFilter(), qos);
    }

    private Set<Integer> supportProtocolLevel() {
        return new HashSet<>(List.of(4));
    }

    private void retain(Publish publish) {
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
            Publish packet = Publish.outgoing(publish.retainFlag(), (byte) publish.qos(),
                publish.dup(), publish.topicName(), (short) 0, publish.payload().copy());
            // save the retained message
            retainPublishManager.add(packet);
        }
    }

    public static final String METRIC_NAME = "broker.node.DefaultBroker";

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
    public void close() throws Exception {
        // Server was started by the Broker
        // So it should not be closed by the Broker
        // todo
    }

    /**
     * use for Aop proxy
     */
    private Broker self;

    public void setSelf(Broker self) {
        this.self = self;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"broker\":\"")
            .append(this.getClass().getSimpleName()).append("@").append(Integer.toHexString(hashCode()))
            .append('\"').append(',');
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }
}
