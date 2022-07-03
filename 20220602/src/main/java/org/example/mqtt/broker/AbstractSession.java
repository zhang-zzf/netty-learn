package org.example.mqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
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

    private final String clientIdentifier;
    private final AtomicInteger pocketIdentifier = new AtomicInteger(0);
    private boolean cleanSession;

    private Queue<ControlPacketContext> inQueue = new ControlPacketContextQueue();
    private Queue<ControlPacketContext> outQueue = new ControlPacketContextQueue();

    private ScheduledFuture retryTask;
    private final int retryPeriod = 0;

    /**
     * bind to the same EventLoop that the channel was bind to.
     */
    private volatile EventLoop eventLoop;
    private Channel channel;

    protected AbstractSession(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
    }

    @Override
    public void close() {
        if (retryTask != null) {
            retryTask.cancel(true);
            retryTask = null;
        }
        if (channel != null) {
            channel.close();
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
        sendInEventLoop(packet);
    }

    protected void sendInEventLoop(ControlPacket packet) {
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

    private void doSendPublish(Publish outgoing) {
        // very little chance
        if (qos2DuplicateCheck(outgoing, outQueue)) {
            return;
        }
        // enqueue
        outQueue.offer(new ControlPacketContext(outgoing, ControlPacketContext.CREATED, OUT));
        // start send some packet
        doSendPublishPacket(outQueue.peek());
        tryCleanQueue(outQueue);
    }

    private boolean qos2DuplicateCheck(Publish packet, Queue<ControlPacketContext> queue) {
        if (packet.exactlyOnce() && existSamePacket(packet, queue)) {
            log.info("qos2 same packet: {}", packet);
            return true;
        }
        return false;
    }

    private boolean existSamePacket(Publish packet, Queue<ControlPacketContext> queue) {
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            if (it.next().equals(packet)) {
                return true;
            }
        }
        return false;
    }

    private void doSendPublishPacket(ControlPacketContext cpx) {
        if (!isBound()) {
            return;
        }
        cpx = findTheFirstCpxToSend(cpx);
        if (cpx == null) {
            return;
        }
        final ControlPacketContext cpxToUse = cpx;
        // mark first and then send data
        cpxToUse.markStatus(ControlPacketContext.CREATED, ControlPacketContext.SENDING);
        // send data and add send complete listener
        doWrite(cpxToUse.packet()).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                cpxToUse.markStatus(ControlPacketContext.SENDING, ControlPacketContext.SENT);
                // send the next packet in the queue if needed
                doSendPublishPacket(cpxToUse.next());
            }
        });
    }

    private ControlPacketContext findTheFirstCpxToSend(ControlPacketContext cpx) {
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
        return cpx;
    }

    private void tryCleanQueue(Queue<ControlPacketContext> queue) {
        // just clean complete cpx from head
        ControlPacketContext cpx = queue.peek();
        // cpx always point to the first cpx in the queue
        while (cpx != null && cpx.complete()) {
            queue.poll();
            callbackAfterPublishRemovedFromQueue(cpx);
            cpx = queue.peek();
        }
        // clean all QoS 0 cpx
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            if (next.complete() && !next.packet().needAck()) {
                callbackAfterPublishRemovedFromQueue(cpx);
                it.remove();
            }
        }
    }

    /**
     * invoke after send Publish successfully.
     *
     * @param controlPacketContext Publish
     */
    protected void callbackAfterPublishRemovedFromQueue(ControlPacketContext controlPacketContext) {
        // noop
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
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue, EXACTLY_ONCE);
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
        tryCleanQueue(outQueue);
    }

    private ControlPacketContext findFirst(Queue<ControlPacketContext> queue, int qos) {
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext cpx = it.next();
            if (cpx.packet().qos() == qos) {
                return cpx;
            }
        }
        return null;
    }

    /**
     * as Sender
     */
    private void doReceivePubRec(PubRec packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue, EXACTLY_ONCE);
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

    /**
     * as Sender
     */
    private void doReceivePubAck(PubAck packet) {
        short packetIdentifier = packet.getPacketIdentifier();
        // only look for the first QoS 1 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(outQueue, Publish.AT_LEAST_ONCE);
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
        tryCleanQueue(outQueue);
    }

    /**
     * as Receiver
     */
    private void doReceivePubRel(PubRel packet) {
        short packetIdentifier = packet.packetIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findFirst(inQueue, EXACTLY_ONCE);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRel nothing
            log.error("Client PubRel nothing. {}, {}", clientIdentifier(), packetIdentifier);
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
            if (future.isSuccess()) {
                cpx.markStatus(PUB_REL, PUB_COMP);
                tryCleanQueue(inQueue);
            }
        });
    }

    /**
     * as Receiver
     */
    private void doReceivePublish(Publish packet) {
        if (packet.needAck() && existSamePacket(packet, inQueue)) {
            log.error("receive same Publish packet: {}", packet.packetIdentifier());
            return;
        }
        ControlPacketContext cpx = new ControlPacketContext(packet, RECEIVED, IN);
        inQueue.offer(cpx);
        // try transfer the packet to all relative subscribers
        doHandleReceivedPublish(packet);
        cpx.markStatus(RECEIVED, ONWARD);
        // now cpx is ONWARD
        if (packet.atMostOnce()) {
            tryCleanQueue(inQueue);
        } else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    cpx.markStatus(ONWARD, PUB_ACK);
                    tryCleanQueue(inQueue);
                }
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

    private ChannelFuture doWrite(ControlPacket packet) {
        startRetryTask();
        return channel.writeAndFlush(packet)
                .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    private void startRetryTask() {
        if (this.retryTask != null) {
            return;
        }
        this.retryTask = eventLoop.scheduleWithFixedDelay(() -> {
            tryCleanQueue(inQueue);
            tryCleanQueue(outQueue);
            if (!isBound() || (inQueue.isEmpty() && outQueue.isEmpty())) {
                // cancel the scheduled task
                this.retryTask.cancel(false);
                this.retryTask = null;
                return;
            }
            doSendTimeoutControlPacket(inQueue.peek());
            doSendTimeoutControlPacket(outQueue.peek());
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void doSendTimeoutControlPacket(ControlPacketContext cpx) {
        if (!isBound()) {
            return;
        }
        // find the first cpx that need retry send
        if (cpx != null && shouldRetrySend(cpx)) {
            doWrite(cpx.retryPacket()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    // resend all the packet
                    doSendTimeoutControlPacket(cpx.next());
                }
            });
        }
    }

    private boolean shouldRetrySend(ControlPacketContext qosPacket) {
        if (retryPeriod == 0) {
            return false;
        }
        return (System.currentTimeMillis() - qosPacket.getMarkedMillis() >= retryPeriod);
    }

    @Override
    public short nextPacketIdentifier() {
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

    public AbstractSession cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    @Override
    public void bind(Channel channel) {
        this.channel = channel;
        this.eventLoop = channel.eventLoop();
        // try start retry task
        this.startRetryTask();
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


    class ControlPacketContextQueue extends AbstractQueue<ControlPacketContext> {

        private final ControlPacketContext head = new ControlPacketContext(null, 0, OUT);
        private ControlPacketContext tail = head;
        private int size;

        @Override
        public Iterator<ControlPacketContext> iterator() {
            return new Iterator<ControlPacketContext>() {
                ControlPacketContext cur = head;
                ControlPacketContext prev = null;
                boolean nexted = false;

                @Override
                public boolean hasNext() {
                    return cur.next() != null;
                }

                @Override
                public ControlPacketContext next() {
                    prev = cur;
                    cur = cur.next();
                    nexted = true;
                    return cur;
                }

                @Override
                public void remove() {
                    if (!nexted) {
                        throw new IllegalStateException();
                    }
                    nexted = false;
                    prev.setNext(cur.next());
                    size -= 1;
                    cur.setNext(null);
                    cur = prev;
                }
            };
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean offer(ControlPacketContext cpx) {
            if (cpx == null) {
                throw new NullPointerException("cpx is null");
            }
            tail.setNext(cpx);
            tail = cpx;
            size += 1;
            return true;
        }

        @Override
        public ControlPacketContext poll() {
            ControlPacketContext node = head.next();
            if (node != null) {
                head.setNext(node.next());
                // good for GC
                node.setNext(null);
                size -= 1;
            }
            if (size == 0) {
                tail = head;
            }
            return node;
        }

        @Override
        public ControlPacketContext peek() {
            return head.next();
        }

    }

}
