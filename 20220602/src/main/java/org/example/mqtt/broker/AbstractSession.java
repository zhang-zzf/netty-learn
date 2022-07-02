package org.example.mqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.mqtt.broker.ControlPacketContext.*;
import static org.example.mqtt.model.ControlPacket.*;
import static org.example.mqtt.model.Publish.EXACTLY_ONCE;

/**
 * QA
 * <pre>
 *     20220627 Session 下无 Channel 时
 *     1. 如何接受 broker 的 send 消息，如何处理？（Persistent Session)
 *     2. 超时调度任务暂定
 *     3.
 * </pre>
 *
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public abstract class AbstractSession implements Session {

    private String clientIdentifier;
    private final AtomicInteger pocketIdentifier = new AtomicInteger(0);
    private boolean cleanSession;

    private final Deque<ControlPacketContext> inQueue = new LinkedList<>();
    private final Deque<ControlPacketContext> outQueue = new LinkedList<>();
    private ScheduledFuture retryTask;
    private final int retryPeriod = 3000;

    /**
     * bind to the same EventLoop that the channel was bind to.
     */
    private EventLoop eventLoop;
    private Channel channel;

    @Override
    public void close() {
        channel.close();
        if (retryTask != null) {
            retryTask.cancel(true);
        }
    }

    protected boolean cleanSession() {
        return cleanSession;
    }

    @Override
    public void send(ControlPacket packet) {
        if (packet == null) {
            return;
        }
        // make sure use the safe thread that the session wad bound to
        if (eventLoop.inEventLoop()) {
            invokeSend(packet);
        } else {
            eventLoop.execute(() -> invokeSend(packet));
        }
    }

    private void invokeSend(ControlPacket packet) {
        // send immediately if can or queue the packet
        // put Publish packet into queue
        if (PUBLISH == packet.type()) {
            doSendPublish((Publish) packet);
        } else {
            doSendPacket(packet);
        }
    }

    private void doSendPacket(ControlPacket packet) {
        if (!isBound()) {
            return;
        }
        doWrite(packet);
    }

    private void doSendPublish(Publish packet) {
        Deque<ControlPacketContext> out = outQueue();
        // very little chance
        if (qos2DuplicateCheck(packet, out)) {
            cleanOutgoingPublish(packet);
            return;
        }
        // generate packetIdentifier for the packet
        Publish outgoing = packet.packetIdentifier(nextPocketIdentifier());
        ControlPacketContext cpx = new ControlPacketContext(outgoing, ControlPacketContext.CREATED);
        offer(out, cpx);
        // start send some packet
        doSendPublishPacket(out.peek());
    }

    private void offer(Deque<ControlPacketContext> queue, ControlPacketContext cpx) {
        // todo : offer failed?
        ControlPacketContext tail = queue.peekLast();
        if (tail != null) {
            tail.setNext(cpx);
        }
        queue.offer(cpx);
    }

    private boolean qos2DuplicateCheck(Publish packet, Queue<ControlPacketContext> queue) {
        if (packet.exactlyOnce() && existSamePacket(packet, queue)) {
            log.info("qos2 same packet: {}", packet);
            return true;
        }
        return false;
    }

    private boolean existSamePacket(Publish packet, Queue<ControlPacketContext> queue) {
        for (ControlPacketContext p : queue) {
            if (p.packet().equals(packet)) {
                return true;
            }
        }
        return false;
    }

    private void doSendPublishPacket(ControlPacketContext cpx) {
        if (!isBound()) {
            return;
        }
        while (true) {
            if (cpx == null) {
                break;
            }
            if (cpx.canPublish()) {
                break;
            }
            if (cpx.inSending()) {
                cpx = null;
                break;
            }
            cpx = cpx.next();
        }
        if (cpx == null) {
            return;
        }
        final ControlPacketContext cpxToUse = cpx;
        // mark first and then send data
        cpxToUse.markStatus(ControlPacketContext.CREATED, ControlPacketContext.SENDING);
        // send data and add send complete listener
        doWrite(cpxToUse.packet()).addListener((ChannelFutureListener) future -> {
            cpxToUse.markStatus(ControlPacketContext.SENDING, ControlPacketContext.SENT);
            cleanQueue(outQueue());
            // send the next packet in the queue if needed
            doSendPublishPacket(cpxToUse.next());
        });
    }

    private void cleanQueue(Deque<ControlPacketContext> queue) {
        ControlPacketContext header = queue.peek();
        while (header != null && header.complete()) {
            // delete the complete cpx
            queue.poll();
            // good fot GC
            header.setNext(null);
            cleanOutgoingPublish(header.packet());
            header = queue.peek();
        }
    }

    private void cleanOutgoingPublish(Publish packet) {
        packet.payload().release();
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

    private void doReceivePubComp(PubComp packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubComp nothing
            log.error("Client PubComp nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubComp the right PacketIdentifier.
            log.error("Client may have lost some PubComp. need: {}, actual: {}, ",
                    cpx.packet().packetIdentifier(), packetIdentifier);
            /* just drop it; */
            return;
        }
        cpx.markStatus(ControlPacketContext.PUB_REC, ControlPacketContext.PUB_COMP);
        // try clean the queue
        cleanQueue(outQueue());
    }

    private ControlPacketContext findFirst(Deque<ControlPacketContext> queue, int qos) {
        ControlPacketContext cpx = queue.peek();
        while (cpx != null) {
            if (cpx.packet().qos() == qos) {
                break;
            }
            cpx = cpx.next();
        }
        return cpx;
    }

    private void doReceivePubRec(PubRec packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRec nothing
            log.error("Client PubRec nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubRec the right PacketIdentifier.
            log.error("Client may have lost some PubRec. need: {}, actual: {}, ",
                    cpx.packet().packetIdentifier(), packetIdentifier);
            /* just drop it; */
            return;
        }
        cpx.markStatus(ControlPacketContext.SENT, ControlPacketContext.PUB_REC);
        // send PubRel packet.
        doWrite(cpx.pubRel());
        // no need clean the queue
    }

    private void doReceivePubAck(PubAck packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 1 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), Publish.AT_LEAST_ONCE);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {
            // Client PubAck nothing
            log.error("Client PubAck nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubAck the right PacketIdentifier.
            log.error("Client may have lost some PubAck. need: {}, actual: {}, ",
                    cpx.packet().packetIdentifier(), packetIdentifier);
            /* just drop it; */
            return;
        }
        cpx.markStatus(ControlPacketContext.SENT, ControlPacketContext.PUB_ACK);
        // try clean the queue
        cleanQueue(outQueue());
    }

    private void doReceivePubRel(PubRel packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRel nothing
            log.error("Client PubRel nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubRel the right PacketIdentifier.
            log.error("Client may have lost some PubRel. need: {}, actual: {}, ",
                    cpx.packet().packetIdentifier(), packetIdentifier);
            /* just drop it; */
            return;
        }
        cpx.markStatus(ONWARD, PUB_REL);
        // ack PubComp to Client
        doWrite(cpx.pubComp()).addListener((ChannelFutureListener) future -> {
            cpx.markStatus(PUB_REL, PUB_COMP);
            // try clean the queue
            cleanQueue(inQueue());
        });
    }

    private void doReceivePublish(Publish packet) {
        Deque<ControlPacketContext> inQueue = inQueue();
        if (packet.needAck() && existSamePacket(packet, inQueue)) {
            log.error("receive same Publish packet: {}", packet.packetIdentifier());
            return;
        }
        ControlPacketContext cpx = new ControlPacketContext(packet, RECEIVED);
        offer(inQueue, cpx);
        // todo : may broke when onward to relative subscriptions
        // try transfer the packet to all relative subscribers
        doHandleReceivedPublish(packet);
        cpx.markStatus(RECEIVED, ONWARD);
        // now cpx is ONWARD
        if (packet.atMostOnce()) {
            cleanQueue(inQueue());
        } else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener((ChannelFutureListener) future -> {
                cpx.markStatus(ONWARD, PUB_ACK);
                cleanQueue(inQueue());
            });
        } else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec());
        }
    }

    /**
     * do handle the Publish from the pair
     *
     * @param packet the Publish packet that received from pair
     */
    protected abstract void doHandleReceivedPublish(Publish packet);

    /**
     * todo queue 实现算法优化
     *
     * @return inQueue
     */
    private Deque<ControlPacketContext> inQueue() {
        return this.inQueue;
    }

    private Deque<ControlPacketContext> outQueue() {
        return this.outQueue;
    }

    private ChannelFuture doWrite(ControlPacket packet) {
        startRetryTask();
        return channel.writeAndFlush(packet);
    }

    private void startRetryTask() {
        if (this.retryTask != null) {
            return;
        }
        this.retryTask = eventLoop.scheduleWithFixedDelay(() -> {
            Deque<ControlPacketContext> in = inQueue();
            Deque<ControlPacketContext> out = outQueue();
            cleanQueue(in);
            cleanQueue(out);
            if (in.isEmpty() && out.isEmpty()) {
                // cancel the scheduled task
                this.retryTask.cancel(false);
                return;
            }
            doRetry(in.peek());
            doRetry(out.peek());
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void doRetry(ControlPacketContext cpx) {
        // find the first cpx that need retry send
        while (cpx != null) {
            if (!cpx.complete() && shouldRetrySend(cpx)) {
                break;
            }
            cpx = cpx.next();
        }
        // resend all the packet that is behind the retry cpx
        while (cpx != null) {
            doSendPacket(cpx.retryPacket());
            cpx = cpx.next();
        }
    }

    private boolean shouldRetrySend(ControlPacketContext qosPacket) {
        return System.currentTimeMillis() - qosPacket.getMarkedMillis() >= retryPeriod;
    }

    protected short nextPocketIdentifier() {
        int id = pocketIdentifier.getAndIncrement();
        if (id >= Short.MAX_VALUE) {
            pocketIdentifier.set(0);
            id = pocketIdentifier.getAndIncrement();
        }
        return (short) id;
    }

    @Override
    public String clientIdentifier() {
        return this.clientIdentifier;
    }

    public AbstractSession clientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
        return this;
    }

    public AbstractSession cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    @Override
    public void bind(Channel channel) {
        this.channel = channel;
        this.eventLoop = channel.eventLoop();
    }

    @Override
    public Channel channel() {
        return this.channel;
    }

    @Override
    public boolean isBound() {
        return channel != null;
    }

}
