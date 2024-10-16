package org.example.mqtt.broker.node;

import io.micrometer.core.annotation.Timed;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.util.*;

import static java.util.stream.Collectors.toSet;
import static org.example.mqtt.model.Publish.NO_PACKET_IDENTIFIER;
import static org.example.mqtt.model.Publish.needAck;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
@Component
public class DefaultBroker implements Broker {

    private final DefaultBrokerState brokerState = new DefaultBrokerState();

    private volatile TopicFilterTree blockedTopicFilter;

    public DefaultBroker() {
        this.self = this;
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
        // must set retain to false before forward the PublishPacket
        packet.retain(false);
        for (Topic topic : brokerState.match(packet.topicName())) {
            for (Map.Entry<ServerSession, Integer> e : topic.subscribers().entrySet()) {
                ServerSession session = e.getKey();
                String topicFilter = topic.topicFilter();
                int qos = qoS(packet.qos(), e.getValue());
                // use a shadow copy of the origin Publish
                short packetIdentifier = packetIdentifier(session, qos);
                Publish outgoing = Publish.outgoing(packet, topicFilter, (byte) qos, packetIdentifier);
                if (log.isDebugEnabled()) {
                    log.debug("Publish({}) forward-> tf: {}, client: {}, packet: {}", packet.pId(), topic.topicFilter(), session.clientIdentifier(), outgoing);
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

    @SneakyThrows
    @Override
    public void destroySession(ServerSession session) {
        log.debug("Broker try to destroySession->{}", session);
        brokerState.disconnect(session).get();
        log.debug("Broker destroyed Session");
    }

    @Override
    public boolean block(Publish packet) {
        return !blockedTopicFilter.match(packet.topicName()).isEmpty();
    }

    @Override
    public void closeSession(ServerSession session) {
        // close channel
        if (session.isBound()) {
            session.channel().close();
        }
        if (session.cleanSession()) {
            String cId = session.clientIdentifier();
            log.debug("Broker({}) now try to close Session({})", this, cId);
            // broker clear session state
            destroySession(session);
            log.debug("Broker closed Session({})", cId);
        }
        // session close callback
        session.onSessionClose();
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

    @SneakyThrows
    @Override
    public void connect(ServerSession session) {
        // sync wait
        ServerSession previous = brokerState.connect(session).get();
        if (previous != null) {
            previous.close();
            log.warn("DefaultBroker#connect close exist Session-> {}", previous);
        }
    }

    private void retain(Publish publish) {
        if (!publish.retain()) {
            throw new IllegalArgumentException();
        }
        Publish packet = publish.copy();
        if (zeroBytesPayload(packet)) {
            log.debug("receive zero bytes payload retain Publish, now remove it: {}", packet);
            // remove the retained message
            brokerState.removeRetain(packet);
        } else {
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
        if (packet.retain()) {
            retain(packet);
        }
        // Broker forward Publish to relative topic after receive a PublishPacket
        self.forward(packet);
    }

    @Override
    public ServerSession createSession(Connect connect) {
        return DefaultServerSession.from(connect);
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

    @Autowired
    public void setSelf(@Qualifier("defaultBroker") Broker self) {
        this.self = self;
    }

}
