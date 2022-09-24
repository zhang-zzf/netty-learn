package org.example.mqtt.broker.node;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.Broker;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;
import org.example.mqtt.model.Unsubscribe;

import java.util.*;

import static org.example.mqtt.model.Publish.NO_PACKET_IDENTIFIER;
import static org.example.mqtt.model.Publish.needAck;

/**
 * @author zhanfeng.zhang
 * @date 2022/06/28
 */
@Slf4j
public class DefaultBroker implements Broker {

    private final DefaultBrokerState brokerState = new DefaultBrokerState();
    private final Map<String, String> listenedServer = new HashMap<>(8);

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
    public void forward(Publish packet) {
        // must set retain to false before forward the PublishPacket
        packet.retain(false);
        for (Topic topic : brokerState.match(packet.topicName())) {
            for (Map.Entry<ServerSession, Integer> e : topic.subscribers().entrySet()) {
                ServerSession session = e.getKey();
                if (!session.isRegistered()) {
                    continue;
                }
                String topicFilter = topic.topicFilter();
                int qos = Math.min(packet.qos(), e.getValue());
                // use a shadow copy of the origin Publish
                short packetIdentifier = needAck(qos) ? session.nextPacketIdentifier() : NO_PACKET_IDENTIFIER;
                Publish outgoing = Publish.outgoing(packet, topicFilter, (byte) qos, packetIdentifier);
                log.debug("Publish({}) forward: {}->{}, {}", packet.pId(), topic.topicFilter(), session.clientIdentifier(), outgoing);
                session.send(outgoing);
            }
        }
    }

    @Override
    public ServerSession session(String clientIdentifier) {
        return brokerState.session(clientIdentifier);
    }

    @SneakyThrows
    @Override
    public void destroySession(ServerSession session) {
        log.debug("Broker try to destroySession->{}", session);
        brokerState.disconnect(session).get();
        log.info("Session was remove from the Broker->{}", session);
    }

    protected Subscribe.Subscription decideSubscriptionQos(ServerSession session, Subscribe.Subscription sub) {
        // todo decide qos
        int qos = sub.qos();
        return new Subscribe.Subscription(sub.topicFilter(), qos);
    }

    @Override
    public void unsubscribe(ServerSession session, Unsubscribe packet) {
        for (Subscribe.Subscription subscription : packet.subscriptions()) {
            brokerState.unsubscribe(session, subscription);
        }
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
        brokerState.connect(session).get();
    }

    @Override
    public void retain(Publish publish) {
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

    @Override
    public Map<String, String> listenedServer() {
        return listenedServer;
    }

    @Override
    public void receiveSysPublish(Publish packet) {
        log.info("Broker receive SysPublish->{}", packet);
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerSession createSession(Connect connect) {
        return DefaultServerSession.from(connect);
    }

    public DefaultBroker listenedServer(String protocol, String url) {
        listenedServer.put(protocol, url);
        return this;
    }

    private boolean zeroBytesPayload(Publish publish) {
        return !publish.payload().isReadable();
    }

    @Override
    public void close() throws Exception {

    }

    public void listenedServer(Map<String, String> protocolToUrl) {
        for (Map.Entry<String, String> entry : protocolToUrl.entrySet()) {
            listenedServer(entry.getKey(), entry.getValue());
        }
    }

}
