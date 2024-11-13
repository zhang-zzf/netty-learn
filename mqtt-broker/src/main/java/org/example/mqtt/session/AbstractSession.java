package org.example.mqtt.session;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static org.example.mqtt.model.ControlPacket.PUBACK;
import static org.example.mqtt.model.ControlPacket.PUBCOMP;
import static org.example.mqtt.model.ControlPacket.PUBLISH;
import static org.example.mqtt.model.ControlPacket.PUBREC;
import static org.example.mqtt.model.ControlPacket.PUBREL;
import static org.example.mqtt.session.ControlPacketContext.Status.HANDLED;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Status.PUB_ACK;
import static org.example.mqtt.session.ControlPacketContext.Status.PUB_COMP;
import static org.example.mqtt.session.ControlPacketContext.Status.PUB_REC;
import static org.example.mqtt.session.ControlPacketContext.Status.PUB_REL;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.PubAck;
import org.example.mqtt.model.PubComp;
import org.example.mqtt.model.PubRec;
import org.example.mqtt.model.PubRel;
import org.example.mqtt.model.Publish;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
@Slf4j
// todo QoS1 / QoS 2 重试发送
public abstract class AbstractSession implements Session {

    public static final ChannelFutureListener LOG_ON_FAILURE = future -> {
        if (!future.isSuccess()) {
            log.error("Channel(" + future.channel() + ").writeAndFlush failed.", future.cause());
        }
    };

    protected final AtomicInteger packetIdentifier = new AtomicInteger(new Random().nextInt(Short.MAX_VALUE));

    private final String clientIdentifier;
    private final Channel channel;

    private final boolean cleanSession;

    /**
     * 是否发送 Publish Packet
     */
    // private volatile Thread sendingPublishThread;
    protected AbstractSession(String clientIdentifier, boolean cleanSession, Channel channel) {
        this.clientIdentifier = checkNotNull(clientIdentifier, "clientIdentifier");
        this.cleanSession = cleanSession;
        this.channel = checkNotNull(channel);
    }

    /**
     * Close the Channel that was used to connect to the client.
     */
    @SneakyThrows
    @Override
    public void close() {
        log.debug("Session({}) try to close itself: {}", cId(), this);
        if (isActive()) {
            log.debug("Session({}) now try to close Channel: {}", cId(), channel);
            channel.close();
            log.debug("Session({}) Channel was closed", cId());
        }
        else {
            log.debug("Session({}) was not bound with a Channel", cId());
        }
    }

    @Override
    public boolean cleanSession() {
        return cleanSession;
    }

    @Override
    public void send(ControlPacket packet) {
        log.debug("send: .->{}, {}", cId(), packet);
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (PUBLISH == packet.type()) {
            invokeSendPublish((Publish) packet);
        }
        else {
            doSendPacket(packet);
        }
    }

    private void doSendPublish(Publish outgoing) {
        // Session 使用 EventLoop 更改内部状态
        assert channel.eventLoop().inEventLoop();
        // very little chance
        if (outQueueQos2DuplicateCheck(outgoing)) {
            log.warn("Session({}) send same Publish(QoS2), discard it: {}", cId(), outgoing);
            return;
        }
        ControlPacketContext cpx = createNewCpx(outgoing, INIT, OUT);
        log.debug("sender({}/{}) Publish .->INIT: {}", cId(), cpx.pId(), cpx);
        if (isActive()) {// isActive. send immediately
            if (enqueueOutQueue(cpx.packet())) {// Only enqueue Qos1 and QoS2
                outQueueEnqueue(cpx);
                if (headOfQueue(cpx, outQueue())) {
                    // cpx is the head of the outQueue
                    doWritePublishPacket(cpx);
                }
                else {
                    log.debug("sender({}/{}) Publish wait for it's turn: {}", cId(), cpx.pId(), cpx);
                }
            }
            // qos0 比 qos1/qos2 更早发送。。？ OK
            else {// no need to enqueue, just send it.
                doWritePublishPacket(cpx);
            }
        }
        else {
            // offline
            if (!enqueueOutQueue(cpx.packet())) {
                // QoS0 Publish will be discarded by default config.
                log.debug("Session({}) is not bind to a Channel, discard the Publish(QoS0): {}", cId(), outgoing);
                publishPacketSentComplete(cpx);
                return;
            }
            // no send, but there is no memory leak
            // todo metric the message
            outQueueEnqueue(cpx);
        }
    }

    private boolean headOfQueue(ControlPacketContext cpx, Queue<ControlPacketContext> queue) {
        return headOfQueue(cpx.packetIdentifier(), queue);
    }

    private boolean headOfQueue(short pId, Queue<ControlPacketContext> queue) {
        ControlPacketContext head = queue.peek();
        return head != null && head.packetIdentifier() == pId;
    }


    private void invokeSendPublish(Publish publish) {
        // make sure use the same thread that the session wad bound to
        if (channel.eventLoop().inEventLoop()) {
            doSendPublish(publish);
        }
        else {
            channel.eventLoop().execute(() -> doSendPublish(publish));
        }
    }

    protected boolean enqueueOutQueue(Publish packet) {
        return packet.atLeastOnce() || packet.exactlyOnce();
    }

    private void outQueueEnqueue(ControlPacketContext cpx) {
        outQueue().offer(cpx);
        log.debug("sender({}/{}) [outQueue enqueue]", cId(), cpx.pId());
    }

    protected String cId() {
        return clientIdentifier();
    }

    protected ChannelFuture doSendPacket(ControlPacket packet) {
        if (!isActive()) {
            log.warn("Session is not bind to a Channel, discard the ControlPacket: {}, {}", cId(), packet);
            return channel.newFailedFuture(new IllegalStateException("Session is not bind to a Channel"));
        }
        return doWrite(packet).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("doSendPacket({}): {}", cId(), packet);
            }
        });
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    private boolean outQueueQos2DuplicateCheck(Publish packet) {
        if (packet.exactlyOnce()) {
            return findControlPacketInOutQueue(packet.packetIdentifier()) != null;
        }
        return false;
    }

    protected ControlPacketContext findControlPacketInOutQueue(short packetIdentifier) {
        for (ControlPacketContext cpx : outQueue()) {
            if (cpx.packet().packetIdentifier() == packetIdentifier) {
                return cpx;
            }
        }
        return null;
    }

    /**
     * 尝试清理 outQueue 中已经发送完成的 cpx
     */
    protected void tryCleanOutQueue() {
        Queue<ControlPacketContext> outQueue = outQueue();
        // just clean complete cpx from head
        ControlPacketContext cpx = outQueue.peek();
        // cpx always point to the first cpx in the queue
        while (cpx != null && cpx.complete()) {
            outQueue.poll();
            log.debug("sender({}/{}) [remove Publish from outQueue]", cId(), cpx.pId());
            publishPacketSentComplete(cpx);
            cpx = outQueue.peek();
        }
    }

    /**
     * 消息接受完成
     *
     * @param cpx the ControlPacketContext
     */
    protected void publishReceivedComplete(ControlPacketContext cpx) {
        log.debug("receiver({}/{}) receive completed", cId(), cpx.pId());
    }

    /**
     * invoke after send Publish complete (maybe discard it)
     *
     * @param cpx Publish
     */
    protected void publishPacketSentComplete(ControlPacketContext cpx) {
        log.debug("sender({}/{}) Publish Packet sent completed", cId(), cpx.pId());
        // send the next if exist
        if (enqueueOutQueue(cpx.packet())) { //? why check -> send the next item in the queue only if cpx is in the queue
            ControlPacketContext head = outQueue().peek();
            log.debug("sender({}) now try to send the next cpx-> {}", cId(), head);
            // outQueue 队列中 head 发送完成，继续发送下一个
            doWritePublishPacket(head);
        }
    }

    @Override
    public void onPacket(ControlPacket packet) {
        switch (packet.type()) {
            case PUBLISH -> doReceivePublish((Publish) packet);
            case PUBACK -> doReceivePubAck((PubAck) packet);
            case PUBREC -> doReceivePubRec((PubRec) packet);
            case PUBREL -> doReceivePubRel((PubRel) packet);
            case PUBCOMP -> doReceivePubComp((PubComp) packet);
            default -> throw new IllegalArgumentException();
        }
    }

    /**
     * as Sender
     */
    private void doReceivePubComp(PubComp packet) {
        log.debug("sender({}/{}) [QoS2 receive PubComp]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {
            // Client PubComp nothing
            log.error("sender({}/{}) PubComp failed, No Publish in outQueue", cId(), packet.pId());
            return;
        }
        // cpx != null means outQueue is not empty
        if (headOfQueue(pId, outQueue())) {
            // 性能优化考虑（Queue 以 DB 实现，可以省去一次 I/O）
            log.debug("sender({}/{}) [PubComp the Header of the outQueue]", cId(), cpx.pId());
            // the header, remove it from the queue
            outQueue().poll();
            log.debug("sender({}/{}) [remove Publish from outQueue]", cId(), cpx.pId());
            publishPacketSentComplete(cpx);
            // try clean the queue (the cpx that behinds head may already complete)
            tryCleanOutQueue();
        }
        else {
            cpx.markStatus(PUB_REC, PUB_COMP);
            log.debug("sender({}/{}) Publish PUB_REC->PUB_COMP", cId(), packet.pId());
        }
    }

    /**
     * as Sender
     */
    private void doReceivePubRec(PubRec packet) {
        log.debug("sender({}/{}) [QoS2 receive PubRec]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {
            // Client PubRec nothing
            log.error("sender({}/{}) PubRec failed. No Publish in outQueue", cId(), packet.pId());
            return;
        }
        cpx.markStatus(PUB_REC);
        log.debug("sender({}/{}) Publish INIT->PUB_REC", cId(), packet.pId());
        // send PubRel packet.
        doWritePubRelPacket(cpx);
    }

    private void doWritePubRelPacket(ControlPacketContext cpx) {
        doWrite(cpx.pubRel()).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("sender({}/{}) [PubRel sent]", cId(), cpx.pId());
            }
        });
    }

    /**
     * as Sender
     */
    private void doReceivePubAck(PubAck packet) {
        log.debug("sender({}/{}) [QoS1 receive PubAck]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {
            // Client PubAck nothing
            log.error("sender({}/{}) PubAck failed, No Publish in outQueue", cId(), packet.pId());
            return;
        }
        if (headOfQueue(pId, outQueue())) {
            // 性能优化考虑（Queue 以 DB 实现，可以省去一次 I/O）
            // the header, just delete it from the queue
            log.debug("sender({}/{}) [PubAck the Header of the outQueue]", cId(), cpx.pId());
            outQueue().poll();
            log.debug("sender({}/{}) [remove Publish from outQueue]", cId(), packet.pId());
            publishPacketSentComplete(cpx);
            // try clean the queue
            tryCleanOutQueue();
        }
        else {
            cpx.markStatus(PUB_ACK);
            log.debug("sender({}/{}) Publish INIT->PUB_ACK", cId(), packet.pId());
        }
    }

    /**
     * as Receiver
     */
    private void doReceivePubRel(PubRel packet) {
        log.debug("receiver({}/{}) [QoS2 receive PubRel]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInInQueue(pId);
        if (cpx == null) {
            // PubRel nothing
            log.error("receiver({}/{}) PubRel failed, No Publish in inQueue", cId(), packet.pId());
            return;
        }
        cpx.markStatus(HANDLED, PUB_REL);
        log.debug("receiver({}/{}) Publish HANDLED->PUB_REL", cId(), cpx.pId());
        // ack PubComp
        doWrite(cpx.pubComp()).addListener(f -> {
            if (f.isSuccess()) {
                cpx.markStatus(PUB_REL, PUB_COMP);
                log.debug("receiver({}/{}) [QoS2 PUB_COMP sent] Publish PUB_REL->PUB_COMP", cId(), cpx.pId());
                tryCleanInQueue();
            }
        });
    }

    protected void tryCleanInQueue() {
        // just clean complete cpx from head
        Queue<ControlPacketContext> inQueue = inQueue();
        // cpx always point to the first cpx in the queue
        ControlPacketContext cpx = inQueue.peek();
        while (cpx != null && cpx.complete()) {
            inQueue.poll();
            log.debug("receiver({}/{}) [remove Publish from inQueue]", cId(), cpx.pId());
            publishReceivedComplete(cpx);
            cpx = inQueue.peek();
        }
    }

    /**
     * as Receiver
     */
    protected void doReceivePublish(Publish packet) {
        log.debug("receiver({}/{}) [receive Publish]: {}", cId(), packet.pId(), packet);
        // QoS 2 duplicate check
        if (packet.exactlyOnce()) {
            ControlPacketContext cpx = findControlPacketInInQueue(packet.packetIdentifier());
            if (cpx != null) {
                log.warn("Session({}) receive same Publish(QoS2) packet: {}, {}", cId(), packet.pId(), cpx);
                doHandleDuplicateQoS2Publish(cpx);
                return;
            }
        }
        // Client / Broker must be online.
        Queue<ControlPacketContext> inQueue = inQueue();
        ControlPacketContext cpx = createNewCpx(packet, INIT, IN);
        log.debug("receiver({}/{}) Publish .->INIT", cId(), cpx.pId());
        if (enqueueInQueue(packet)) {
            inQueue.offer(cpx);
            log.debug("receiver({}/{}) [inQueue enqueue]", cId(), cpx.pId());
        }
        // handle the PublishPacket
        onPublish(packet);
        // now cpx is HANDLED
        cpx.markStatus(INIT, HANDLED);
        log.debug("receiver({}/{}) [Publish Handled] Publish INIT->HANDLED", cId(), cpx.pId());
        if (packet.atMostOnce()) {
            publishReceivedComplete(cpx);
        }
        else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener(f -> {
                cpx.markStatus(HANDLED, PUB_ACK);
                log.debug("receiver({}/{}) [QoS1 PUB_ACK sent] Publish HANDLED->PUB_ACK", cId(), cpx.pId());
                publishReceivedComplete(cpx);
            });
        }
        else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec()).addListener(f -> log.debug("receiver({}/{}) [QoS2 PUB_REC sent]", cId(), cpx.pId()));
        }
    }

    protected boolean enqueueInQueue(Publish packet) {
        return packet.exactlyOnce();
    }

    protected ControlPacketContext createNewCpx(Publish packet,
        ControlPacketContext.Status status,
        ControlPacketContext.Type type) {
        return new ControlPacketContext(packet, status, type);
    }

    private void doHandleDuplicateQoS2Publish(ControlPacketContext cpx) {
        switch (cpx.status()) {
            case INIT:
                log.debug("receiver({}/{}) Publish INIT->. [QoS2 重复消息，inQueue 队列中状态为 INIT]: {}", cId(), cpx.pId(), cpx);
                break;
            case HANDLED:
                doWritePubRecPacket(cpx);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void doWritePubRecPacket(ControlPacketContext cpx) {
        // does not modify the status of the cpx
        doWrite(cpx.pubRec()).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("receiver({}/{}) HANDLED ->. [QoS2 已发送 PUB_REC]: {}", cId(), cpx.pId(), cpx);
            }
        });
    }

    protected ControlPacketContext findControlPacketInInQueue(short packetIdentifier) {
        for (ControlPacketContext cpx : inQueue()) {
            if (cpx.packet().packetIdentifier() == packetIdentifier) {
                return cpx;
            }
        }
        return null;
    }

    /**
     * do handle the Publish from the pair
     * <p>the Session receive a Publish packet</p>
     *
     * @param packet the Publish packet that received from pair
     */
    protected abstract void onPublish(Publish packet);

    private ChannelFuture doWrite(ControlPacket packet) {
        return channel.writeAndFlush(packet)
            .addListener(FIRE_EXCEPTION_ON_FAILURE)
            .addListener(LOG_ON_FAILURE)
            ;
    }

    @Override
    public short nextPacketIdentifier() {
        int id = packetIdentifier.incrementAndGet();
        if (id >= Short.MAX_VALUE) {
            packetIdentifier.set(Short.MIN_VALUE);
            id = packetIdentifier.getAndIncrement();
        }
        return (short) id;
    }

    @Override
    public String clientIdentifier() {
        return this.clientIdentifier;
    }

    /**
     * CONNECT -> CONNACK
     * <pre>
     *     callback after established  CONNECT -> CONNACK
     * </pre>
     */
    public void connected() {
        // invoke later
        channel.eventLoop().submit(this::resendOutQueue);
    }

    protected void resendOutQueue() {
        tryCleanOutQueue();
        // resend cpx
        resendCpxInOutQueue(outQueue().peek());
    }

    protected void resendCpxInOutQueue(ControlPacketContext cpx) {
        if (cpx == null) {
            return;
        }
        Publish packet = cpx.packet();
        if (packet.atMostOnce() || packet.atLeastOnce()) {
            cpx.markStatus(INIT);
            doWritePublishPacket(cpx);
        }
        else if (packet.exactlyOnce()) {
            switch (cpx.status()) {
                case INIT -> doWritePublishPacket(cpx);
                // send PubRel packet.
                case PUB_REC -> doWritePubRelPacket(cpx);
                default -> throw new IllegalStateException();
            }
        }
    }

    private ChannelFuture doWritePublishPacket(ControlPacketContext cpx) {
        if (cpx == null) {
            return channel.newSucceededFuture();
        }
        return doWrite(cpx.packet()).addListener(f -> {
            if (f.isSuccess()) {
                publishPacketSent(cpx);
            }
            if (!enqueueOutQueue(cpx.packet())) {
                publishPacketSentComplete(cpx);
            }
        });
    }

    public static final String METRIC_NAME = AbstractSession.class.getName();

    protected void publishPacketSent(ControlPacketContext cpx) {
        log.debug("sender({}/{}) [Publish sent]", cId(), cpx.pId());
    }

    @Override
    public Channel channel() {
        return this.channel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        if (clientIdentifier != null) {
            sb.append("\"clientIdentifier\":\"").append(clientIdentifier).append('\"').append(',');
        }
        return sb.replace(sb.length() - 1, sb.length(), "}").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractSession that = (AbstractSession) o;
        return clientIdentifier.equals(that.clientIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientIdentifier);
    }

    protected abstract Queue<ControlPacketContext> inQueue();

    protected abstract Queue<ControlPacketContext> outQueue();

    @Override
    public AbstractSession migrate(Session session) {
        if (session instanceof AbstractSession as) {
            // packet id
            this.packetIdentifier.set(as.packetIdentifier.get());
        }
        return this;
    }

}
