package org.example.mqtt.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
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
            log.debug("Session({}) now try to unbind from Channel: {}", cId(), channel);
            if (channel.isOpen()) {
                channel.close();
            }
            channel = null;
            channelClosed();
            log.debug("Session({}) unbound from Channel", cId());
        } else {
            log.debug("Session({}) was not bound with a Channel", cId());
        }
    }

    protected void channelClosed() {
        log.debug("Session({}) was closed.", cId());
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
        log.debug("send: .->{}, {}", cId(), packet);
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
            log.warn("Session({}) send same Publish(QoS2), discard it: {}", cId(), outgoing);
            return;
        }
        ControlPacketContext cpx = createNewCpx(outgoing, INIT, OUT);
        log.debug("sender({}/{}) Publish .->INIT: {}", cId(), cpx.pId(), cpx);
        if (isBound()) {
            // online
            // Only enqueue Qos1 and QoS2
            if (enqueueOutQueue(cpx.packet())) {
                outQueueEnqueue(cpx);
            }
            // online. send immediately
            doWritePublishPacket(cpx);
        } else {
            // offline
            if (outgoing.atMostOnce() && !sendQoS0PublishOffline()) {
                // QoS0 Publish will be discarded by default config.
                log.debug("Session({}) is not bind to a Channel, discard the Publish(QoS0): {}", cId(), outgoing);
                publishPacketSentComplete(cpx);
                return;
            }
            outQueueEnqueue(cpx);
        }
    }

    protected boolean enqueueOutQueue(Publish packet) {
        return packet.atLeastOnce() || packet.exactlyOnce();
    }

    private void outQueueEnqueue(ControlPacketContext cpx) {
        outQueue().offer(cpx);
        log.debug("sender({}/{}) [outQueue enqueue]", cId(), cpx.pId());
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
    }

    @Override
    public void onPacket(ControlPacket packet) {
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
                log.error("unhandled ControlPacket type->{}", packet);
                throw new IllegalArgumentException();
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
        if (outQueue().peek().packetIdentifier() == pId) {
            // 性能优化考虑（Queue 以 DB 实现，可以省去一次 I/O）
            log.debug("sender({}/{}) [PubComp the Header of the outQueue]", cId(), cpx.pId());
            // the header, remove it from the queue
            outQueue.poll();
            log.debug("sender({}/{}) [remove Publish from outQueue]", cId(), cpx.pId());
            publishPacketSentComplete(cpx);
            // try clean the queue
            tryCleanOutQueue();
        } else {
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
        if (outQueue().peek().packetIdentifier() == pId) {
            // 性能优化考虑（Queue 以 DB 实现，可以省去一次 I/O）
            // the header, just delete it from the queue
            log.debug("sender({}/{}) [PubAck the Header of the outQueue]", cId(), cpx.pId());
            outQueue().poll();
            log.debug("sender({}/{}) [remove Publish from outQueue]", cId(), packet.pId());
            publishPacketSentComplete(cpx);
            // try clean the queue
            tryCleanOutQueue();
        } else {
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
        } else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener(f -> {
                cpx.markStatus(HANDLED, PUB_ACK);
                log.debug("receiver({}/{}) [QoS1 PUB_ACK sent] Publish HANDLED->PUB_ACK", cId(), cpx.pId());
                publishReceivedComplete(cpx);
            });
        } else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec()).addListener(f -> {
                log.debug("receiver({}/{}) [QoS2 PUB_REC sent]", cId(), cpx.pId());
            });
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
                log.debug("receiver({}/{}) Publish INIT->. [QoS2 重复消息，inQueue 队列中状态为 INIT]: {}, {}", cId(), cpx.pId(), cpx);
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
                log.debug("receiver({}/{}) HANDLED ->. [QoS2 已发送 PUB_REC]: {}, {}", cId(), cpx.pId(), cpx);
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
        log.debug("Session({}) now try bind to Channel: {}", cId(), channel);
        while (sendingPublishThread != null && !channel.eventLoop().inEventLoop(sendingPublishThread)) {
            // spin
        }
        this.eventLoop = channel.eventLoop();
        // better: eventLoop first then channel
        this.channel = channel;
        // try start retry task
        // send Publish from outQueue immediately.
        this.eventLoop.submit(this::resendOutQueue);
        log.debug("Session({}) now bound to Channel", cId());
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
                publishPacketSent(cpx);
            }
            // must release the retained Publish
            if (!enqueueOutQueue(cpx.packet())) {
                publishPacketSentComplete(cpx);
            }
        }).addListener(f -> {
            if (!f.isSuccess()) {
                // todo send packet failed -> memory leak
            }
        });
    }

    private void publishPacketSent(ControlPacketContext cpx) {
        log.debug("sender({}/{}) [Publish sent]", cId(), cpx.pId());
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

    protected Queue<ControlPacketContext> inQueue() {
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
