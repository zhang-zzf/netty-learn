package org.example.mqtt.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static org.example.mqtt.model.ControlPacket.*;
import static org.example.mqtt.session.ControlPacketContext.Status.*;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public abstract class AbstractSession implements Session {

    public static final ChannelFutureListener LOG_ON_FAILURE = future -> {
        if (!future.isSuccess()) {
            log.error("Channel(" + future.channel() + ").writeAndFlush failed.", future.cause());
        }
    };

    private final String clientIdentifier;
    protected final AtomicInteger packetIdentifier = new AtomicInteger(new Random().nextInt(Short.MAX_VALUE));
    private Boolean cleanSession;

    private Queue<ControlPacketContext> inQueue;
    private Queue<ControlPacketContext> outQueue;

    /**
     * bind to the same EventLoop that the channel was bind to.
     */
    private volatile EventLoop eventLoop;
    private volatile Channel channel;

    /**
     * 是否发送 Publish Packet
     */
    private volatile Thread sendingPublishThread;

    protected AbstractSession(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
    }

    /**
     * Close the Channel that was used to connect to the client.
     */
    @Override
    public void closeChannel() {
        if (isBound()) {
            if (channel.isOpen()) {
                channel.close();
            }
            channel = null;
        }
    }

    @Override
    public boolean cleanSession() {
        if (cleanSession == null) {
            return true;
        }
        return cleanSession;
    }

    @Override
    public void send(ControlPacket packet) {
        log.debug("send: {}, {}", cId(), packet);
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (PUBLISH == packet.type()) {
            sendPublishInEventLoop((Publish) packet);
        } else {
            doSendPacket(packet);
        }
    }

    protected void sendPublishInEventLoop(Publish publish) {
        if (eventLoop == null) {
            throw new IllegalStateException();
        }
        // make sure use the safe thread that the session wad bound to
        if (eventLoop.inEventLoop()) {
            invokeSendPublish(publish);
        } else {
            eventLoop.execute(() -> invokeSendPublish(publish));
        }
    }

    private void invokeSendPublish(Publish packet) {
        // send immediately if can or queue the packet
        // put Publish packet into queue
        try {
            sendingPublishThread = Thread.currentThread();
            doSendPublish(packet);
        } finally {
            sendingPublishThread = null;
        }
    }

    private void doSendPublish(Publish outgoing) {
        // very little chance
        if (outQueueQos2DuplicateCheck(outgoing)) {
            log.warn("Session({}) send same Publish(QoS2), discard it.: {}", cId(), outgoing);
            return;
        }
        ControlPacketContext cpx = createNewCpx(outgoing, INIT, OUT);
        if (isBound()) {
            // online
            // Only enqueue Qos1 and QoS2
            if (enqueueOutQueue(cpx)) {
                outQueueEnqueue(cpx);
            }
            // online. send immediately
            doWritePublishPacket(cpx);
        } else {
            // offline
            if (outgoing.atMostOnce() && !sendQoS0PublishOffline()) {
                // QoS0 Publish will be discarded by default config.
                log.debug("Session({}) is not bind to a Channel, discard the Publish(QoS0): {}", cId(), outgoing);
                publishSendComplete(cpx);
                return;
            }
            outQueueEnqueue(cpx);
        }
    }

    private boolean enqueueOutQueue(ControlPacketContext cpx) {
        Publish packet = cpx.packet();
        return packet.atLeastOnce() || packet.exactlyOnce();
    }

    private void outQueueEnqueue(ControlPacketContext cpx) {
        outQueue().offer(cpx);
        log.debug("sendPublish({}) .->INIT [outQueue 入队]: {}, {}", cpx.pId(), cId(), cpx);
    }

    /**
     * 以下情况下是否需要发送 QoS0 消息
     * <p>Client 与 Broker 不存在连接</p>
     * <p>Broker 与 Client(非 CleanSession ）不存在连接</p>
     */
    protected boolean sendQoS0PublishOffline() {
        return false;
    }

    protected String cId() {
        return clientIdentifier();
    }

    private void doSendPacket(ControlPacket packet) {
        if (!isBound()) {
            log.warn("Session is not bind to a Channel, discard the ControlPacket: {}, {}", cId(), packet);
            return;
        }
        doWrite(packet).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("doSendPacket({}): {}", cId(), packet);
            }
        });
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
            publishSendComplete(cpx);
            cpx = outQueue.peek();
        }
    }

    /**
     * 消息接受完成
     *
     * @param cpx the ControlPacketContext
     */
    protected void qoS2PublishReceived(ControlPacketContext cpx) {
        log.debug("receivePublish({}) received [消息接受完成，从 inQueue 中移除]: {}, {}", cpx.pId(), cId(), cpx);
    }

    /**
     * invoke after send Publish complete (maybe discard it)
     *
     * @param cpx Publish
     */
    protected void publishSendComplete(ControlPacketContext cpx) {
        if (!enqueueOutQueue(cpx)) {
            log.debug("sendPublish({}) sent [消息发送完成]: {}, {}", cpx.pId(), cId(), cpx);
        }
        log.debug("sendPublish({}) sent [消息发送完成，从 outQueue 中移除]: {}, {}", cpx.pId(), cId(), cpx);
    }

    @Override
    public void messageReceived(ControlPacket packet) {
        switch (packet.type()) {
            case PUBLISH:
                doReceivePublish((Publish) packet);
                break;
            case PUBACK:
                doReceivePubAck((PubAck) packet);
                break;
            case PUBREC:
                doReceivePubRec((PubRec) packet);
                break;
            case PUBREL:
                doReceivePubRel((PubRel) packet);
                break;
            case PUBCOMP:
                doReceivePubComp((PubComp) packet);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * as Sender
     */
    private void doReceivePubComp(PubComp packet) {
        log.debug("receivePubComp: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {
            // Client PubComp nothing
            log.error("Session({}) PubComp nothing. {}, queue: {}", clientIdentifier(), pId, outQueue());
            return;
        }
        cpx.markStatus(PUB_REC, PUB_COMP);
        log.debug("sendPublish({}) PUB_REC->PUB_COMP [QoS2 收到 Client PUB_COMP]: {}, {}", packet.pId(), cId(), packet);
        // try clean the queue
        tryCleanOutQueue();
    }

    /**
     * as Sender
     */
    private void doReceivePubRec(PubRec packet) {
        log.debug("receivePubRec: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {
            // Client PubRec nothing
            log.error("Session({}) PubRec nothing. {}, queue: {}", cId(), pId, outQueue());
            return;
        }
        cpx.markStatus(SENT, PUB_REC);
        log.debug("sendPublish({}) SENT->PUB_REC [QoS2 收到 Client PUB_REC]: {}, {}", packet.pId(), cId(), cpx);
        // send PubRel packet.
        doWritePubRelPacket(cpx);
    }

    private void doWritePubRelPacket(ControlPacketContext cpx) {
        doWrite(cpx.pubRel()).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("sendPublish({}) PUB_REC->. [QoS2 发送 PUB_REL 给 Client]: {}, {}", cpx.pId(), cId(), cpx);
            }
        });
    }

    /**
     * as Sender
     */
    private void doReceivePubAck(PubAck packet) {
        log.debug("receivePubAck: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {
            // Client PubAck nothing
            log.error("Session({}) PubAck nothing. {}, queue: {}", cId(), pId, outQueue());
            return;
        }
        cpx.markStatus(SENT, PUB_ACK);
        log.debug("sendPublish({}) SENT->PUB_ACK: {}, {}", pId, cId(), cpx);
        // try clean the queue
        tryCleanOutQueue();
    }

    /**
     * as Receiver
     */
    private void doReceivePubRel(PubRel packet) {
        log.debug("receivePubRel: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInInQueue(pId);
        if (cpx == null) {
            // PubRel nothing
            log.error("Session({}) PubRel nothing. {}", cId(), pId);
            return;
        }
        cpx.markStatus(HANDLED, PUB_REL);
        log.debug("receivePublish({}) HANDLED->PUB_REL [QoS2 收到 PUB_REL]: {}, {}", cpx.pId(), cId(), cpx);
        // ack PubComp
        doWrite(cpx.pubComp()).addListener(f -> {
            if (f.isSuccess()) {
                cpx.markStatus(PUB_REL, PUB_COMP);
                log.debug("receivePublish({}) PUB_REL->PUB_COMP [QoS2 已发送 PUB_COMP]: {}, {}", cpx.pId(), cId(), cpx);
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
            qoS2PublishReceived(cpx);
            cpx = inQueue.peek();
        }
    }

    /**
     * as Receiver
     */
    protected void doReceivePublish(Publish packet) {
        log.debug("receivePublish({}): {}, {}", packet.pId(), cId(), packet);
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
        if (packet.exactlyOnce()) {
            // Only QoS2 need enqueue
            inQueue.offer(cpx);
            log.debug("receivePublish({}) .->INIT [inQueue 入队]: {}, {}", cpx.pId(), cId(), cpx);
        }
        // handle the PublishPacket
        onPublish(packet);
        // now cpx is HANDLED
        cpx.markStatus(INIT, HANDLED);
        log.debug("receivePublish({}) INIT->HANDLED: {}, {}", cpx.pId(), cId(), cpx);
        if (packet.atLeastOnce()) {
            // QoS1
            doWrite(cpx.pubAck()).addListener(f -> {
                if (f.isSuccess()) {
                    cpx.markStatus(HANDLED, PUB_ACK);
                    log.debug("receivePublish({}) HANDLED->PUB_ACK [QoS1 消息已处理且已发送 PUB_ACK]: {}, {}", cpx.pId(), cId(), cpx);
                }
            });
        } else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec()).addListener(f -> {
                if (f.isSuccess()) {
                    log.debug("receivePublish({}) HANDLED ->. [QoS2 消息已处理且已发送 PUB_REC]: {}, {}", cpx.pId(), cId(), cpx);
                }
            });
        }
    }

    protected ControlPacketContext createNewCpx(Publish packet,
                                                ControlPacketContext.Status status,
                                                ControlPacketContext.Type type) {
        return new ControlPacketContext(packet, status, type);
    }

    private void doHandleDuplicateQoS2Publish(ControlPacketContext cpx) {
        switch (cpx.status()) {
            case INIT:
                log.debug("receivePublish({}) INIT->. [QoS2 重复消息，inQueue 队列中状态为 INIT]: {}, {}", cpx.pId(), cId(), cpx);
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
                log.debug("receivePublish({}) HANDLED ->. [QoS2 已发送 PUB_REC]: {}, {}", cpx.pId(), cId(), cpx);
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
    protected abstract boolean onPublish(Publish packet);

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

    public AbstractSession cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    @Override
    public void bind(Channel channel) {
        while (sendingPublishThread != null && !channel.eventLoop().inEventLoop(sendingPublishThread)) {
            // spin
        }
        this.eventLoop = channel.eventLoop();
        // better: eventLoop first then channel
        this.channel = channel;
        // try start retry task
        // send Publish from outQueue immediately.
        this.eventLoop.submit(this::resendOutQueue);
    }

    protected void resendOutQueue() {
        tryCleanOutQueue();
        Queue<ControlPacketContext> outQueue = outQueue();
        for (ControlPacketContext cpx : outQueue) {
            resendCpxInOutQueue(cpx);
        }
    }

    protected void resendCpxInOutQueue(ControlPacketContext cpx) {
        Publish packet = cpx.packet();
        if (packet.atMostOnce() || packet.atLeastOnce()) {
            cpx.markStatus(INIT);
            doWritePublishPacket(cpx);
        } else if (packet.exactlyOnce()) {
            switch (cpx.status()) {
                case INIT:
                case SENT:
                    cpx.markStatus(INIT);
                    doWritePublishPacket(cpx);
                    break;
                case PUB_REC:
                    // send PubRel packet.
                    doWritePubRelPacket(cpx);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private void doWritePublishPacket(ControlPacketContext cpx) {
        doWrite(cpx.packet()).addListener(f -> {
            if (f.isSuccess()) {
                cpx.markStatus(INIT, SENT);
                log.debug("sendPublish({}) INIT->SENT: {}, {}", cpx.pId(), cId(), cpx);
            }
            // must release the retained Publish
            if (!enqueueOutQueue(cpx)) {
                publishSendComplete(cpx);
            }
        });
    }

    @Override
    public Channel channel() {
        return this.channel;
    }

    @Override
    public boolean isBound() {
        return channel != null;
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

    private Queue<ControlPacketContext> inQueue() {
        if (inQueue == null) {
            inQueue = newInQueue();
        }
        return inQueue;
    }

    protected Queue<ControlPacketContext> newInQueue() {
        return new LinkedList<>();
    }

    protected Queue<ControlPacketContext> outQueue() {
        if (outQueue == null) {
            outQueue = newOutQueue();
        }
        return outQueue;
    }

    protected Queue<ControlPacketContext> newOutQueue() {
        return new LinkedList<>();
    }

}
