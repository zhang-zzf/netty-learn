package org.github.zzf.mqtt.server;

import static java.util.Optional.ofNullable;
import static org.github.zzf.mqtt.protocol.model.Connect.PROTOCOL_LEVEL_3_1_1;
import static org.github.zzf.mqtt.protocol.model.Connect.PROTOCOL_LEVEL_5_0;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.CONNECT;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.DISCONNECT;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PINGREQ;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.SUBSCRIBE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.UNSUBSCRIBE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.validateTopicName;
import static org.github.zzf.mqtt.protocol.model.Publish.META_NM_RECEIVE;
import static org.github.zzf.mqtt.protocol.model.Publish.META_NM_WRAP;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_RECEIVE_MILLIS;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_RECEIVE_NANO;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_SOURCE;
import static org.github.zzf.mqtt.protocol.model.Publish.META_P_SOURCE_BROKER;
import static org.github.zzf.mqtt.protocol.model.Publish.NO_PACKET_IDENTIFIER;
import static org.github.zzf.mqtt.protocol.model.Publish.needAck;
import static org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50.RETAIN_HANDLING_NOT_SEND;
import static org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50.RETAIN_HANDLING_SEND_IF_NEW;
import static org.github.zzf.mqtt.protocol.model.Subscribe.Subscription.V50.RETAIN_HANDLING_SEND_RETAIN;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ConnAck;
import org.github.zzf.mqtt.protocol.model.Connect;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.ControlPacket.MalformedPacketException;
import org.github.zzf.mqtt.protocol.model.ControlPacket.Properties;
import org.github.zzf.mqtt.protocol.model.Disconnect;
import org.github.zzf.mqtt.protocol.model.PingReq;
import org.github.zzf.mqtt.protocol.model.PingResp;
import org.github.zzf.mqtt.protocol.model.Publish;
import org.github.zzf.mqtt.protocol.model.SubAck;
import org.github.zzf.mqtt.protocol.model.Subscribe;
import org.github.zzf.mqtt.protocol.model.Subscribe.Subscription;
import org.github.zzf.mqtt.protocol.model.UnsubAck;
import org.github.zzf.mqtt.protocol.model.Unsubscribe;
import org.github.zzf.mqtt.protocol.server.Authenticator;
import org.github.zzf.mqtt.protocol.server.Broker;
import org.github.zzf.mqtt.protocol.server.ServerSession;
import org.github.zzf.mqtt.protocol.session.AbstractSession;
import org.github.zzf.mqtt.protocol.session.ControlPacketContext;
import org.github.zzf.mqtt.server.metric.MetricUtil;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-06
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    public static final String METRIC_NAME = DefaultServerSession.class.getName();
    static final Set<Byte> SUPPORTED_PROTOCOL_LEVEL = Set.of(PROTOCOL_LEVEL_3_1_1);

    final Broker broker;
    Map<String, Subscription> subscriptions = new HashMap<>();
    // todo 监控内存占用
    Queue<ControlPacketContext> inQueue = new LinkedList<>();
    Queue<ControlPacketContext> outQueue = new LinkedList<>();
    Connect connect;
    ConnAck connAck;
    boolean disconnect;

    // resume flag
    final AtomicReference<DefaultServerSession> resumeSession = new AtomicReference<>();

    public DefaultServerSession(Broker broker, Channel channel) {
        super(channel);
        this.broker = broker;
    }

    /**
     * Will Message
     * <pre>
     *     initiate by Connect if will flag is present. It will be cleaned after
     *     1. receive Disconnect
     *     2. lost the Channel to Client, and forward the message to relative Topic.
     * </pre>
     */
    private Publish extractWillMessage(Connect connect) {
        int qos = connect.willQos();
        String topic = connect.willTopic();
        ByteBuf byteBuf = connect.willMessage();
        boolean retain = connect.willRetainFlag();
        return Publish.outgoing(retain, (byte) qos, false,
                topic, (short) 0,
                byteBuf);
    }

    @Override
    public String clientIdentifier() {
        return ofNullable(connect).map(Connect::clientIdentifier).orElse(null);
    }

    @Override
    public ChannelFuture write(ControlPacket packet) {
        // todo
        if (resumeSession.get() != null) {
            // this session has been resumed
            return resumeSession.get().write(packet);
        }
        if (packet instanceof Publish publish) {
            // retain 不会创建新的对象
            publish.payload().retain();
            log.debug("sender({}/{}) Publish . -> [RETAIN] payload.refCnt: {}",
                    cId(), publish.pId(), publish.payload().refCnt());
        }
        return super.write(packet);
    }

    @Override
    public void sessionRead(ControlPacket packet) {
        switch (packet.type()) {
            case CONNECT -> doReceiveConnect((Connect) packet);
            case PINGREQ -> doReceivePingReq((PingReq) packet);
            case SUBSCRIBE -> doReceiveSubscribe((Subscribe) packet);
            case UNSUBSCRIBE -> doReceiveUnsubscribe((Unsubscribe) packet);
            case DISCONNECT -> doReceiveDisconnect((Disconnect) packet);
            default -> super.sessionRead(packet);
        }
    }

    protected void doReceiveConnect(Connect packet) {
        String clientId = packet.clientIdentifier();
        log.debug("Session({}) << Connect {}", clientId, packet);
        if (this.connect != null) {
            /* A Client can only send the CONNECT Packet once over a Network Connection.
            The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client
            */
            log.error("Session({}) >> CloseSession : client send Connect packet more than once", clientId);
            this.close();
            return;
        }
        // The Server MUST respond to the CONNECT Packet
        // with a CONNACK return code 0x01 (unacceptable protocol level) and then
        // disconnect the Client if the Protocol Level is not supported by the Server
        if (!supportProtocolLevel(packet)) {
            writeNotSupportedProtocolLevelConnack();
            this.close();
            log.error("Session({}) >> CloseSession : protocolLevel: {} is not supported", clientId, packet.protocolLevel());
            return;
        }
        // authenticate may be async
        byte authenticate = broker.authenticate(packet);
        if (authenticate != Authenticator.AUTHENTICATE_SUCCESS) {
            // todo metric
            log.error("Session({}) >> CloseSession : authenticate failed", clientId);
            writeAuthenticateFailedConnAck(authenticate);
            this.close();
            return;
        }
        // now can accept the Connect
        this.connect = packet;
        ServerSession previous = broker.session(clientId);
        if (previous == null) {// case 1: broker has no Session, create a new session
            broker.connect(this).thenRun(this::writeAcceptedConnAck);
        }
        else {
            closeExistSession(previous).whenComplete((unused, throwable) -> {
                // watch out: channel.eventLoop().inEventLoop() may be false
                ServerSession closedPrevious = broker.session(clientId);
                if (sessionNotResumable(closedPrevious)) {// case 2: previous session is clean, create a new session
                    broker.connect(this).thenRun(this::writeAcceptedConnAck);
                }
                else {
                    if (resumeSession(packet)) {// case 3: resume session
                        resume(closedPrevious);
                        broker.connect(this) // first connect the session
                                .thenCompose((v) -> broker.disconnect(previous)) // then disconnect the previous session
                                .thenRun(this::writeAcceptedWithStoredSessionConnAck)
                                .exceptionally(t -> resumeSessionFailed(t, clientId));
                    }
                    else { // case 4: not cleanSession -> cleanSession
                        broker.disconnect(closedPrevious)
                                .thenCompose(v -> broker.connect(this))
                                .thenRun(this::writeAcceptedConnAck)
                                .exceptionally(t -> notCleanSessionToCleanSessionFailed(t, clientId));
                    }
                }
            });
        }
    }

    private Void notCleanSessionToCleanSessionFailed(Throwable t, String clientId) {
        log.error("Session({}) >> CloseSession : disconnect previous session failed", clientId, t);
        this.close();
        return null;
    }

    private Void resumeSessionFailed(Throwable t, String clientId) {
        log.error("Session({}) >> CloseSession : resume session failed", clientId, t);
        this.close();
        return null;
    }

    protected boolean resumeSession(Connect packet) {
        return !packet.cleanSession();
    }

    protected boolean sessionNotResumable(ServerSession session) {
        return session == null;
    }

    protected CompletionStage<Void> closeExistSession(ServerSession previous) {
        previous.close();
        return previous.closeFuture();
    }

    protected void writeAcceptedConnAck() {
        this.writeConnAck(ConnAck.accepted());
    }

    protected void writeAcceptedWithStoredSessionConnAck() {
        this.writeConnAck(ConnAck.acceptedWithStoredSession());
    }

    protected void writeAuthenticateFailedConnAck(byte authenticate) {
        this.write(ConnAck.authenticateFailed(authenticate));
    }

    protected void writeNotSupportedProtocolLevelConnack() {
        this.write(ConnAck.notSupportProtocolLevel());
    }

    protected boolean supportProtocolLevel(Connect packet) {
        return SUPPORTED_PROTOCOL_LEVEL.contains(packet.protocolLevel());
    }

    // must run in this.channel().eventLoop()
    protected void writeConnAck(ConnAck packet) {
        doInEventLoop(() -> {
            this.connAck = packet;
            this.write(packet);
            this.sessionActive();
        });
    }

    // must run in this.channel().eventLoop()

    private void resume(ServerSession session) {
        if (!(session instanceof DefaultServerSession dss)) {
            throw new UnsupportedOperationException();
        }
        // resume previous session
        // The Client and Server MUST store the Session after the Client and Server are disconnected
        // 按照 mqtt 协议 Client 会保存和重新发送未确认的消息。若 Client 未按照协议设计可能导致 inQueue 异常
        // todo inQueue 存在 QoS2 消息，若 Client 重连后没有恢复 QoS2 消息的状态，inQueue 中的消息和后续消息接无法清理
        // todo Client 重连后，如何重发 QoS1 / QoS2 消息 ？
        // 1. QoS1 重新发送消息； QoS2 按状态恢复发送
        // 1. QoS1 重新发送消息； QoS2 重新发送
        // mqtt 协议规定:
        // When a Client reconnects with CleanSession set to 0, both the Client and Server
        // MUST re-send any unacknowledged PUBLISH Packets (where QoS > 0) and
        // PUBREL Packets using their original Packet Identifiers.
        // 通用理解为：  QoS1 重新发送消息； QoS2 按状态恢复发送
        // sender 未收到 PUBREC ->  客户端必须重新发送相同的 PUBLISH 数据包（相同的 Packet ID=100） DUP 标志设置为 1
        // sender 收到 PUBREC ->  客户端必须重新发送 PUBREL 数据包（相同的 Packet ID=100）
        // resume packetIdentifier
        this.packetIdentifier.set(dss.nextPacketIdentifier());
        // resume subscriptions
        this.subscriptions = dss.subscriptions;
        // resume inQueue and outQueue
        this.inQueue = dss.inQueue;
        this.outQueue = dss.outQueue;
        // change the previous session's resume flag to this session
        dss.resumeSession.set(this);
    }

    @Override
    protected void doReceivePublish(Publish packet) {
        // client to server, validate topic name
        if (!validateTopicName(packet.topicName())) {
            throw new MalformedPacketException();
        }
        super.doReceivePublish(packet);
    }

    private void doReceivePingReq(PingReq packet) {
        write(PingResp.from());
        log.debug("Session({}) PingReq -> PingResp", cId());
    }

    @Override
    public Set<Subscription> subscriptions() {
        return new HashSet<>(subscriptions.values());
    }

    @Override
    protected void onPublish(Publish packet) {
        broker.forward(clientIdentifier(), packet);
    }

    private ChannelFuture write(boolean retain,
            Subscription subscription,
            Publish packet) {
        int qos = Math.min(packet.qos(), subscription.qos());
        short packetIdentifier = needAck(qos) ? nextPacketIdentifier() : NO_PACKET_IDENTIFIER;
        // use a shadow copy of the origin Publish
        Publish outgoing = Publish.outgoing(
                retain, qos, false,
                packet.topicName(), packetIdentifier,
                /* packet.payload().slice());  verified: must use slice()*/
                packet.payload());  /** {@link Publish#toByteBuf()} compositeBuffer take over the ownership of the payload's ByteBuf, so the payload's ByteBuf will not change it's readerIdx / writerIdx */
        return write(outgoing);
    }

    @Override
    public ChannelFuture forward(String clientId, String topicFilter, Publish packet) {
        Subscription subscription = subscriptions.get(topicFilter);
        if (subscription == null) {
            log.error("Session({}) >> forward: Subscription({}) not found", cId(), topicFilter);
            return channel().newSucceededFuture();
        }
        /* must set retain to false before forward the Publish Packet */
        return write(false, subscription, packet);
    }

    @Override
    protected Queue<ControlPacketContext> inQueue() {
        return inQueue;
    }

    @Override
    protected Queue<ControlPacketContext> outQueue() {
        return outQueue;
    }

    @Override
    protected void publishSent(Publish packet) {
        try {
            metricPublish(packet);
        } catch (Throwable e) {
            // just log an error
            log.error("unExpected exception", e);
        }
        super.publishSent(packet);
    }

    private void metricPublish(Publish packet) {
        Map<String, Object> meta = packet.meta();
        if (meta == null) {
            return;
        }
        long now = System.currentTimeMillis();
        Long pReceive = (Long) meta.get(META_P_RECEIVE_MILLIS);
        if (META_P_SOURCE_BROKER.equals(meta.get(META_P_SOURCE))) {
            // Publish may come from another Broker
            Long nmWrap = (Long) meta.get(META_NM_WRAP);
            Long nmReceive = (Long) meta.get(META_NM_RECEIVE);
            // packetReceive->nmWrap 和 packetReceive->.->packetSent 使用同样的流程，不再重复打点
            // MetricUtil.time(METRIC_NAME, nmWrap - pReceive, "phase", "packetReceive->nmWrap");
            MetricUtil.time(METRIC_NAME, nmReceive - nmWrap, "phase", "nmWrap->nmReceive");
            // nmReceive->packetSent 和 packetReceive->.->packetSent 使用同样的流程，不再重复打点
            // MetricUtil.time(METRIC_NAME, now - nmReceive, "phase", "nmReceive->packetSent");
            MetricUtil.time(METRIC_NAME, now - pReceive, "phase", "packetReceive->nm->packetSent");
        }
        else {
            long nanoTime = System.nanoTime();
            long pReceiveInNano = (long) meta.get(META_P_RECEIVE_NANO);
            MetricUtil.nanoTime(METRIC_NAME, nanoTime - pReceiveInNano, "phase", "packetReceive->.->packetSent");
        }
        // Public come from Client directly or through another Broker
        // the whole time between Publish.Receive from Client and forward to another Client.
        if (pReceive != null) {
            MetricUtil.time(METRIC_NAME, now - pReceive, "phase", "packetReceive->packetSent");
        }
    }

    protected void doReceiveSubscribe(Subscribe packet) {
        log.debug("Session({}) << Subscribe: {}", cId(), packet);
        // 2 things: add to this.subscriptions and retain message
        broker.subscribe(this, packet)
                // triggerRetainMessage will exec after send SubAck
                .thenCompose(v -> doWriteSubAck(packet, v))
                // triggerRetainMessage must used after doWriteSubAck
                // async send retain Publish
                .thenApply(this::triggerRetainMessage)
        ;
    }

    private List<Subscription> triggerRetainMessage(List<Subscription> granted) {
        String[] tfs = granted.stream().map(Subscription::topicFilter).toArray(String[]::new);
        broker.retainedPublish(tfs).thenAccept(publishPackets -> {
            if (retainedMessageIsEmpty(publishPackets)) {
                return;
            }
            doInEventLoop(() -> writeRetainPublish(publishPackets));
        });
        return granted;
    }

    boolean retainedMessageIsEmpty(Map<String, List<Publish>> publishPackets) {
        if (publishPackets.isEmpty()) {
            return true;
        }
        for (List<Publish> list : publishPackets.values()) {
            if (!list.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void writeRetainPublish(Map<String, List<Publish>> publishPackets) {
        for (Entry<String, List<Publish>> e : publishPackets.entrySet()) {
            Subscription subscription = subscriptions.get(e.getKey());
            if (subscription == null) {
                return;
            }
            for (Publish publish : e.getValue()) {
                log.debug("Session({}) >> SendRetainPublish: {} - {}", cId(), e.getKey(), publish);
                write(true, subscription, publish);
            }
        }
    }

    private CompletionStage<List<Subscription>> doWriteSubAck(Subscribe packet,
            List<Integer> reasonCodes) {
        CompletableFuture<List<Subscription>> stage = new CompletableFuture<>();
        SubAck subAck = SubAck.from(packet.packetIdentifier(), reasonCodes);
        //
        List<Subscription> granted = packet.grantSubscription(reasonCodes);
        doInEventLoop(() -> {
            for (Subscription s : granted) {
                subscriptions.put(s.topicFilter(), s);
            }
            log.debug("Session({}) >> SubAck: {}", cId(), subAck);
            write(subAck).addListener(f -> stage.complete(granted));
        });
        return stage;
    }

    protected void doReceiveUnsubscribe(Unsubscribe packet) {
        log.info("Session({}) << Unsubscribe: {}", cId(), packet);
        broker.unsubscribe(this, packet.subscriptions());
        packet.subscriptions().forEach(this.subscriptions::remove);
        UnsubAck unsubAck = UnsubAck.from(packet.packetIdentifier());
        log.info("Session({}) >> UnsubAck: {}", cId(), unsubAck);
        doWrite(unsubAck);
    }

    private void doReceiveDisconnect(Disconnect packet) {
        log.debug("Session({}) << Disconnect", clientIdentifier());
        this.disconnect = true;
        channel().close();
    }

    @Override
    protected CompletionStage<Void> doCleanSession() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        doInEventLoop(() -> {
            if (connect == null) {
                future.complete(null);
                return;
            }
            if (!disconnect && connect.willFlag()) {
                Publish willMessage = extractWillMessage(connect);
                log.debug("Session({}) closed before Disconnect, now send Will: {}", cId(), willMessage);
                onPublish(willMessage);
            }
            if (connect.cleanSession()) {
                broker.disconnect(this).whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    }
                    else {
                        future.complete(null);
                    }
                });
            }
            else {
                future.complete(null);
            }
        });
        return future;
    }

    public static class V50 extends DefaultServerSession {

        static final Set<Byte> SUPPORTED_PROTOCOL_LEVEL = Set.of(PROTOCOL_LEVEL_5_0);

        /* 连接断开时间戳 */
        volatile Long disconnectMillis;

        // Topic Alias mappings exist only within a Network Connection and last only for the lifetime of that Network Connection
        // A receiver MUST NOT carry forward any Topic Alias mappings from one Network Connection to another.
        final Map<Integer, String> topicAliasMap = new HashMap<>();

        public V50(Broker broker, Channel channel) {
            super(broker, channel);
        }

        protected boolean resumeSession(Connect packet) {
            return !((Connect.V50) packet).cleanStart();
        }

        protected boolean sessionNotResumable(ServerSession session) {
            if (session instanceof V50 v50) {// mqtt5.0
                return v50.isExpired();
            }
            else {// mqtt3.1 or null
                // mqtt3.1 update to mqtt5.0
                return session == null;
            }
        }

        // if the ClientID represents a Client already connected to the Server, the Server sends a
        // DISCONNECT packet to the existing Client with Reason Code of 0x8E (Session taken over)
        // and MUST close the Network Connection of the existing Client
        protected CompletionStage<Void> closeExistSession(ServerSession previous) {
            if (previous instanceof V50 v50) {
                if (v50.channel().isActive()) {
                    v50.write(Disconnect.V50.sessionTakenOver());
                }
            }
            previous.close();
            return previous.closeFuture();
        }

        protected void writeAcceptedConnAck() {
            this.writeConnAck(ConnAck.V50.accepted());
        }

        protected void writeAcceptedWithStoredSessionConnAck() {
            this.writeConnAck(ConnAck.V50.acceptedWithStoredSession());
        }

        protected void writeAuthenticateFailedConnAck(byte authenticate) {
            this.write(ConnAck.V50.authenticateFailed(authenticate));
        }

        protected void writeNotSupportedProtocolLevelConnack() {
            this.write(ConnAck.V50.notSupportProtocolLevel());
        }

        protected boolean supportProtocolLevel(Connect packet) {
            return SUPPORTED_PROTOCOL_LEVEL.contains(packet.protocolLevel());
        }

        @Override
        protected void doReceiveSubscribe(Subscribe packet) {
            log.debug("Session({}) << Subscribe: {}", cId(), packet);
            // register the Subscribe packet
            // 2 things: add to this.subscriptions and retain message
            // Retain Handling option
            Map<String, Boolean> retainHandling = retainHandlingOption(packet);
            broker.subscribe(this, packet)
                    .thenCompose(v -> doWriteSubAck(packet, v))
                    // async send retain Publish
                    .thenApply(grantedSub -> triggerRetainMessage(grantedSub, retainHandling))
            ;
        }

        private List<Subscription> triggerRetainMessage(
                List<Subscription> granted,
                Map<String, Boolean> retainHandling) {
            String[] tfs = granted.stream()
                    .map(Subscription::topicFilter)
                    .filter(tf -> retainHandling.getOrDefault(tf, false))
                    .toArray(String[]::new);
            broker.retainedPublish(tfs).thenAccept(publishPackets -> {
                if (retainedMessageIsEmpty(publishPackets)) {
                    return;
                }
                doInEventLoop(() -> writeRetainPublish(publishPackets));
            }).exceptionally(t -> {
                log.debug("Session({}) >> SendRetainPublish failed}", cId(), t);
                return null;
            });
            return granted;
        }

        private void writeRetainPublish(Map<String, List<Publish>> publishPackets) {
            for (Entry<String, List<Publish>> e : publishPackets.entrySet()) {
                Subscription.V50 subscription = (Subscription.V50) subscriptions.get(e.getKey());
                if (subscription == null) {
                    return;
                }
                for (Publish publish : e.getValue()) {
                    log.debug("Session({}) >> SendRetainPublish: {} - {}", cId(), e.getKey(), publish);
                    write(true, subscription, publish);
                }
            }
        }

        private Map<String, Boolean> retainHandlingOption(Subscribe packet) {
            Map<String, Boolean> retainHandling = new HashMap<>();
            List<Subscription> subscriptionList = packet.subscriptions();
            for (Subscription s : subscriptionList) {
                Subscription.V50 sub = (Subscription.V50) s;
                final String tf = sub.topicFilter();
                switch (sub.retainHandling()) {
                    case RETAIN_HANDLING_SEND_RETAIN -> retainHandling.put(tf, true);
                    case RETAIN_HANDLING_SEND_IF_NEW -> retainHandling.put(tf, !subscriptions.containsKey(tf));
                    case RETAIN_HANDLING_NOT_SEND -> retainHandling.put(tf, false);
                }
            }
            return retainHandling;
        }

        private CompletionStage<List<Subscription>> doWriteSubAck(Subscribe packet,
                List<Integer> reasonCodes) {
            CompletableFuture<List<Subscription>> stage = new CompletableFuture<>();
            SubAck.V50 subAck = SubAck.V50.from(packet.packetIdentifier(), reasonCodes, Properties.empty());
            List<Subscription> granted = packet.grantSubscription(reasonCodes);
            doInEventLoop(() -> {
                for (Subscription s : granted) {
                    this.subscriptions.put(s.topicFilter(), s);
                }
                log.debug("Session({}) >> SubAck: {}", cId(), subAck);
                write(subAck).addListener(f -> stage.complete(granted));
            });
            return stage;
        }

        @Override
        protected void doReceivePublish(Publish packet) {
            if (packet instanceof Publish.V50 publish) {
                Publish.V50 handledPacket = doHandleTopicAlias(publish);
                super.doReceivePublish(handledPacket);
            }
            else {
                throw new MalformedPacketException();
            }
        }

        private Publish.V50 doHandleTopicAlias(Publish.V50 publish) {
            Optional<Integer> topicAlias = publish.properties().topicAlias();
            if (topicAlias.isPresent()) {
                if (topicAlias.get() > ((ConnAck.V50) connAck).topicAliasMaximum()) {
                    throw new MalformedPacketException();
                }
                String topicName = publish.topicName();
                if (topicName.isEmpty()) {
                    String mappedTopicName = topicAliasMap.get(topicAlias.get());
                    if (mappedTopicName == null) {
                        throw new MalformedPacketException();
                    }
                    return Publish.V50.updateTopicName(publish, mappedTopicName);
                }
                else {
                    topicAliasMap.putIfAbsent(topicAlias.get(), topicName);
                    return publish;
                }
            }
            else {
                return publish;
            }
        }

        @Override
        public ChannelFuture forward(String clientId, String topicFilter, Publish packet) {
            Subscription.V50 subscription = (Subscription.V50) subscriptions.get(topicFilter);
            if (subscription == null) {
                log.error("Session({}) >> forward: Subscription({}) not found", cId(), topicFilter);
                return this.channel().newSucceededFuture();
            }
            // No Local Option
            if (subscription.noLocal() && clientId.equals(clientIdentifier())) {
                log.debug("Session({}) >> forward: NoLocalOption", cId());
                return this.channel().newSucceededFuture();
            }
            // Retain As Published option
            boolean retainFlag = subscription.retainAsPublished() ? packet.retainFlag() : false;
            return write(retainFlag, subscription, packet);
        }

        private ChannelFuture write(boolean retain,
                Subscription.V50 subscription,
                Publish packet) {
            int qos = Math.min(packet.qos(), subscription.qos());
            short packetIdentifier = needAck(qos) ? nextPacketIdentifier() : NO_PACKET_IDENTIFIER;
            Properties properties;
            if (packet instanceof Publish.V50 publishV50) {
                properties = publishV50.properties();
                // 3.3.2.3.3 Message Expiry Interval
                Optional<Long> messageLifetimeLeft = messageLifetimeLeft(publishV50);
                if (messageLifetimeLeft.isPresent()) {
                    if (messageLifetimeLeft.get() < 0L) {
                        return channel().newSucceededFuture();
                    }
                    properties.messageExpiryInterval(messageLifetimeLeft.get());
                }
            }
            else {
                properties = Properties.empty();
            }
            // add Subscription Identifier to Properties
            subscription.identifier().ifPresent(properties::subscriptionIdentifier);
            // use a shadow copy of the origin Publish
            Publish.V50 outgoing = Publish.V50.outgoing(
                    retain, qos, false,
                    packet.topicName(), packetIdentifier, properties,
                    /* packet.payload().slice());  verified: must use slice()*/
                    packet.payload());  /** {@link Publish#toByteBuf()} compositeBuffer take over the ownership of the payload's ByteBuf, so the payload's ByteBuf will not change it's readerIdx / writerIdx */
            return write(outgoing);
        }


        private Optional<Long> messageLifetimeLeft(Publish.V50 packet) {
            Optional<Long> messageExpiryInterval = packet.properties().messageExpiryInterval();
            if (messageExpiryInterval.isPresent()) {
                long expire = System.currentTimeMillis() - packet.timestamp();
                return Optional.of(messageExpiryInterval.get() - expire / 1000);
            }
            return Optional.empty();
        }

        private boolean isExpired() {
            if (disconnectMillis == null) {
                return false;
            }
            Connect.V50 connect = (Connect.V50) this.connect;
            long sessionDisconnectPeriod = System.currentTimeMillis() - disconnectMillis;
            return sessionDisconnectPeriod >= connect.sessionExpiryInterval() * 1000;
        }

        @Override
        public void sessionInactive() {
            disconnectMillis = System.currentTimeMillis();
            super.sessionInactive();
        }

    }

}
