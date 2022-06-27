package org.example.mqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.Deque;
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

    private final AtomicInteger pocketIdentifier = new AtomicInteger(0);
    private ScheduledFuture retryTask;
    private final int retryPeriod = 3000;

    private final EventLoop eventLoop;
    private Channel channel;

    /**
     * whether the Session disconnect with the Client
     */
    private boolean disconnect;
    private final AbstractBroker broker;

    private String clientIdentifier;
    private int keepAlive;
    private boolean cleanSession;

    protected AbstractSession(Channel channel, AbstractBroker broker) {
        this.channel = channel;
        this.eventLoop = channel.eventLoop();
        this.broker = broker;
    }

    @Override
    public void close() {
        if (!persistent()) {
            // disconnect the session from the broker
            broker().disconnect(this);
        }
        disconnect = true;
        channel = null;
        if (retryTask != null) {
            retryTask.cancel(true);
        }
    }

    @Override
    public void send(ControlPacket packet) {
        // make sure use the safe thread that the session wad bound to
        if (eventLoop.inEventLoop()) {
            invokeSend(packet);
        } else {
            eventLoop.execute(() -> invokeSend(packet));
        }
    }

    private void invokeSend(ControlPacket packet) {
        if (packet == null) {
            return;
        }
        // send immediately if can or queue the packet
        // put Publish packet into queue
        if (PUBLISH == packet.type()) {
            doSendPublish((Publish) packet);
        } else {
            doSendPacket(packet);
        }
    }

    private void doSendPacket(ControlPacket packet) {
        if (disconnect()) {
            return;
        }
        doWrite(packet);
    }

    private void doSendPublish(Publish packet) {
        Deque<ControlPacketContext> out = outQueue();
        // very little chance
        if (qos2DuplicateCheck(packet, out)) {
            return;
        }
        // generate packetIdentifier for the packet
        Publish outgoing = Publish.outgoing(packet, nextPocketIdentifier());
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

    private boolean qos2DuplicateCheck(Publish packet, Queue<ControlPacketContext> out) {
        if (packet.exactlyOnce()) {
            for (ControlPacketContext p : out) {
                if (p.packet().equals(packet)) {
                    // same packet
                    log.info("qos2 same packet: {}", packet);
                    return true;
                }
            }
        }
        return false;
    }

    private void doSendPublishPacket(ControlPacketContext cpx) {
        if (disconnect()) {
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

    private boolean disconnect() {
        return disconnect;
    }

    private void cleanQueue(Deque<ControlPacketContext> queue) {
        ControlPacketContext header = queue.peek();
        while (header != null && header.complete()) {
            // delete the complete cpx
            queue.poll();
            // good for gc
            header.setNext(null);
            header = queue.peek();
        }
    }

    @Override
    public void receive(ControlPacket packet) {
        switch (packet.type()) {
            case PUBLISH:
                doReceivePublish((Publish) packet);
                break;
            case PUBREL:
                doReceivePubRel((PubRel) packet);
                break;
            case PUBACK:
                doReceivePubAck((PubAck) packet);
                break;
            case PUBREC:
                doReceivePubRec((PubRec) packet);
                break;
            case PUBCOMP:
                doReceivePubComp((PubComp) packet);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    protected void doReceivePubComp(PubComp packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubComp nothing
            log.error("Client PubComp nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().getPacketIdentifier() != packetIdentifier) {
            // Client does not PubComp the right PacketIdentifier.
            log.error("Client may have lost some PubComp. need: {}, actual: {}, ",
                    cpx.packet().getPacketIdentifier(), packetIdentifier);
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
        if (cpx.packet().getPacketIdentifier() != packetIdentifier) {
            // Client does not PubRec the right PacketIdentifier.
            log.error("Client may have lost some PubRec. need: {}, actual: {}, ",
                    cpx.packet().getPacketIdentifier(), packetIdentifier);
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
        if (cpx.packet().getPacketIdentifier() != packetIdentifier) {
            // Client does not PubAck the right PacketIdentifier.
            log.error("Client may have lost some PubAck. need: {}, actual: {}, ",
                    cpx.packet().getPacketIdentifier(), packetIdentifier);
            /* just drop it; */
            return;
        }
        cpx.markStatus(ControlPacketContext.SENT, ControlPacketContext.PUB_ACK);
        // try clean the queue
        cleanQueue(outQueue());
    }

    protected void doReceivePubRel(PubRel packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue(), EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRel nothing
            log.error("Client PubRel nothing. {}", packetIdentifier);
            return;
        }
        if (cpx.packet().getPacketIdentifier() != packetIdentifier) {
            // Client does not PubRel the right PacketIdentifier.
            log.error("Client may have lost some PubRel. need: {}, actual: {}, ",
                    cpx.packet().getPacketIdentifier(), packetIdentifier);
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

    protected void doReceivePublish(Publish packet) {
        Deque<ControlPacketContext> inQueue = inQueue();
        if (packet.needAck()) {
            for (ControlPacketContext qosPacket : inQueue) {
                if (packet.equals(qosPacket.packet())) {
                    // todo
                    return;
                }
            }
        }
        ControlPacketContext cpx = new ControlPacketContext(packet, RECEIVED);
        offer(inQueue, cpx);
        // try transfer all the packet to relative subscribers
        doReceive(cpx);
    }

    private void doReceive(ControlPacketContext cpx) {
        // todo : may broke when onward to relative subscriptions
        Publish packet = cpx.packet();
        broker().onward(packet);
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
     * todo queue 实现算法优化
     *
     * @return inQueue
     */
    protected abstract Deque<ControlPacketContext> inQueue();

    protected abstract Deque<ControlPacketContext> outQueue();

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
    public Broker broker() {
        return this.broker;
    }

    @Override
    public boolean persistent() {
        return !this.cleanSession;
    }

    @Override
    public String clientIdentifier() {
        return this.clientIdentifier;
    }

    AbstractSession clientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
        return this;
    }

    AbstractSession keepAlive(int keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    AbstractSession persistent(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

}
