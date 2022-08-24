package org.example.mqtt.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.mqtt.model.ControlPacket.*;
import static org.example.mqtt.model.Publish.*;
import static org.example.mqtt.session.ControlPacketContext.*;

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
    private final AtomicInteger pocketIdentifier = new AtomicInteger(new Random().nextInt(Short.MAX_VALUE));
    private boolean cleanSession;

    private final Queue<ControlPacketContext> inQueue = new ControlPacketContextQueue();
    private final Queue<ControlPacketContext> outQueue = new ControlPacketContextQueue();

    // protected by eventLoop.thread
    private ScheduledFuture<?> retryTask;
    private final int retryPeriodInMillis = 60 * 1000;

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

    @Override
    public void close() {
        if (retryTask != null) {
            retryTask.cancel(true);
            retryTask = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
        for (ControlPacketContext cpx : outQueue) {
            if (cpx.inSending()) {
                // re mark the SENDING cpx
                cpx.markStatus(SENDING, INIT);
                break;
            }
        }
    }

    @Override
    public boolean cleanSession() {
        return cleanSession;
    }

    @Override
    public Future<Void> send(ControlPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        if (PUBLISH == packet.type()) {
            return sendPublishInEventLoop((Publish) packet);
        } else {
            DefaultPromise<Void> promise = newPromise();
            doSendPacket(packet, promise);
            return promise;
        }
        // return sendInEventLoop(packet);
    }

    protected Promise<Void> sendPublishInEventLoop(Publish publish) {
        if (eventLoop == null) {
            throw new IllegalStateException();
        }
        DefaultPromise<Void> promise = newPromise();
        // make sure use the safe thread that the session wad bound to
        if (eventLoop.inEventLoop()) {
            invokeSendPublish(publish, promise);
        } else {
            eventLoop.execute(() -> invokeSendPublish(publish, promise));
        }
        return promise;
    }

    private void invokeSendPublish(Publish packet, Promise<Void> promise) {
        // send immediately if can or queue the packet
        // put Publish packet into queue
        try {
            sendingPublishThread = Thread.currentThread();
            doSendPublish(packet, promise);
        } finally {
            sendingPublishThread = null;
        }
    }

    private void doSendPublish(Publish outgoing, Promise<Void> promise) {
        // Client offline check for QoS 0 (atMostOnce) Publish packet
        if (!isBound() && outgoing.atMostOnce()) {
            promise.tryFailure(new IllegalStateException("Client is Down"));
            return;
        }
        // very little chance
        if (qos2DuplicateCheck(outgoing, outQueue)) {
            promise.trySuccess(null);
            log.info("Session({}) send same qos2 packet: {}", clientIdentifier(), outgoing);
            return;
        }
        // enqueue
        outQueue.offer(new ControlPacketContext(outgoing, ControlPacketContext.INIT, OUT, promise));
        // start send some packet
        doSendPublishAndClean();
    }

    private void doSendPublishAndClean() {
        doSendPublishPacket(outQueue.peek());
        tryCleanOutQueue();
        if (!outQueue.isEmpty()) {
            startRetryTask();
        }
    }

    private void doSendPacket(ControlPacket packet, Promise<Void> promise) {
        if (!isBound()) {
            promise.tryFailure(new IllegalStateException("Session was not bound to Channel"));
            return;
        }
        doWrite(packet).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                promise.trySuccess(null);
            } else {
                promise.tryFailure(future.cause());
            }
        });
    }

    private boolean qos2DuplicateCheck(Publish packet, Queue<ControlPacketContext> queue) {
        return packet.exactlyOnce() && existSamePacket(packet, queue);
    }

    private boolean existSamePacket(Publish packet, Queue<ControlPacketContext> queue) {
        for (ControlPacketContext controlPacketContext : queue) {
            if (controlPacketContext.packet().equals(packet)) {
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
        cpxToUse.markStatus(INIT, SENDING);
        // there is no concurrency problem, but the doWrite is async progress.
        // send data and add send complete listener
        doWrite(cpxToUse.packet()).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                cpxToUse.markStatus(SENDING, SENT);
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

    private void tryCleanOutQueue() {
        // just clean complete cpx from head
        ControlPacketContext cpx = outQueue.peek();
        // cpx always point to the first cpx in the queue
        while (cpx != null && cpx.complete()) {
            outQueue.poll();
            publishSent(cpx);
            cpx = outQueue.peek();
        }
        Iterator<ControlPacketContext> it = outQueue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            int qos = next.packet().qos();
            if (qos == AT_MOST_ONCE) {
                // clean all QoS 0 cpx
                it.remove();
                publishSent(next);
            } else if (controlPacketTimeout(next)) {
                // clean all timeout cpx
                it.remove();
                log.warn("Session({}) clean timeout Publish({}) in outQueue after {}ms.",
                        clientIdentifier(), next, next.elapseAfterLastMark());
                next.timeout();
            }
        }
    }

    private void tryCleanInQueue() {
        // just clean complete cpx from head
        ControlPacketContext cpx = inQueue.peek();
        // cpx always point to the first cpx in the queue
        while (cpx != null && cpx.complete()) {
            inQueue.poll();
            publishReceived(cpx);
            cpx = inQueue.peek();
        }
        Iterator<ControlPacketContext> it = inQueue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            int qos = next.packet().qos();
            if ((qos == AT_MOST_ONCE) || (qos == AT_LEAST_ONCE)) {
                // clean all QoS0 / QoS1 cpx
                it.remove();
                publishReceived(next);
            } else if (controlPacketTimeout(next)) {
                // clean all timeout cpx
                it.remove();
                log.warn("Session({}) clean timeout Publish({}) in inQueue after {}ms",
                        clientIdentifier(), next, next.elapseAfterLastMark());
                next.timeout();
            }
        }
    }

    /**
     * 消息接受完成
     *
     * @param cpx the ControlPacketContext
     */
    protected void publishReceived(ControlPacketContext cpx) {
        cpx.success();
    }

    private boolean controlPacketTimeout(ControlPacketContext cpx) {
        if (retryPeriodInMillis == 0) {
            return false;
        }
        return (cpx.elapseAfterLastMark() >= 2 * retryPeriodInMillis);
    }

    /**
     * invoke after send Publish successfully.
     *
     * @param cpx Publish
     */
    protected void publishSent(ControlPacketContext cpx) {
        cpx.success();
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
            log.error("Session({}) PubComp nothing. {}, queue: {}", clientIdentifier(), packetIdentifier, outQueue);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubComp the right PacketIdentifier.
            log.error("Session({}) may have lost some PubComp. need: {}, actual: {}, queue: {}",
                    clientIdentifier(), cpx.packet().packetIdentifier(), packetIdentifier, outQueue);
            /* just drop it; */
            return;
        }
        cpx.markStatus(PUB_REC, PUB_COMP);
        // try clean the queue
        tryCleanOutQueue();
    }

    private ControlPacketContext findFirst(Queue<ControlPacketContext> queue, int qos) {
        for (ControlPacketContext cpx : queue) {
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
            log.error("Session({}) PubRec nothing. {}, queue: {}",
                    clientIdentifier(), packetIdentifier, outQueue);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubRec the right PacketIdentifier.
            log.error("Session({}) may have lost some PubRec. need: {}, actual: {}, queue: {}", clientIdentifier,
                    cpx.packet().packetIdentifier(), packetIdentifier, outQueue);
            /* just drop it; */
            return;
        }
        cpx.markStatus(SENT, PUB_REC);
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
        ControlPacketContext cpx = findFirst(outQueue, AT_LEAST_ONCE);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {
            // Client PubAck nothing
            log.error("Session({}) PubAck nothing. {}, queue: {}", clientIdentifier(), packetIdentifier, outQueue);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubAck the right PacketIdentifier.
            log.error("Session({}) may have lost some PubAck. need: {}, actual: {}, queue: {}",
                    clientIdentifier(), cpx.packet().packetIdentifier(), packetIdentifier, outQueue);
            /* just drop it; */
            return;
        }
        cpx.markStatus(SENT, PUB_ACK);
        // try clean the queue
        tryCleanOutQueue();
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
            log.error("Session({}) PubRel nothing. {}, queue: {}", clientIdentifier(), packetIdentifier, inQueue);
            return;
        }
        if (cpx.packet().packetIdentifier() != packetIdentifier) {
            // Client does not PubRel the right PacketIdentifier.
            log.error("Session({}) may have lost some PubRel. need: {}, actual: {}, queue: {} ",
                    clientIdentifier(), cpx.packet().packetIdentifier(), packetIdentifier, inQueue);
            /* just drop it; */
            return;
        }
        cpx.markStatus(HANDLED, PUB_REL);
        // ack PubComp to Client
        doWrite(cpx.pubComp()).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                cpx.markStatus(PUB_REL, PUB_COMP);
                tryCleanInQueue();
            }
        });
    }

    /**
     * as Receiver
     */
    protected void doReceivePublish(Publish packet) {
        if (packet.needAck() && existSamePacket(packet, inQueue)) {
            log.warn("Session({}) receive same Publish packet: {}", clientIdentifier(), packet.packetIdentifier());
            return;
        }
        Promise<Void> receiveProgress = newPromise();
        ControlPacketContext cpx = new ControlPacketContext(packet, INIT, IN, receiveProgress);
        inQueue.offer(cpx);
        boolean handled = false;
        try {
            handled = onPublish(packet, receiveProgress);
        } catch (Exception e) {
            log.error("Session({}) throw exception when handle Publish", clientIdentifier(), e);
        }
        if (!handled) {
            log.error("Session({}) handle Publish failed", clientIdentifier());
            // todo
            // wait for retry
            // current no retry
            // return;
        }
        cpx.markStatus(INIT, HANDLED);
        // now cpx is HANDLED
        if (packet.atMostOnce()) {
            tryCleanInQueue();
        } else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    cpx.markStatus(HANDLED, PUB_ACK);
                    tryCleanInQueue();
                }
            });
        } else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec());
        }
    }

    private DefaultPromise<Void> newPromise() {
        return new DefaultPromise<>(this.eventLoop);
    }

    /**
     * do handle the Publish from the pair
     * <p>the Session receive a Publish packet</p>
     *
     * @param packet the Publish packet that received from pair
     */
    protected abstract boolean onPublish(Publish packet, Future<Void> promise);

    private ChannelFuture doWrite(ControlPacket packet) {
        return channel.writeAndFlush(packet)
                .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    private void startRetryTask() {
        if (!isBound() || this.retryTask != null) {
            return;
        }
        this.retryTask = eventLoop.scheduleWithFixedDelay(() -> {
            boolean emptyQueue = inQueue.isEmpty() && outQueue.isEmpty();
            if (!isBound() || emptyQueue) {
                // cancel the scheduled task
                this.retryTask.cancel(false);
                this.retryTask = null;
                return;
            }
            retrySendControlPacket(inQueue.peek());
            retrySendControlPacket(outQueue.peek());
            tryCleanInQueue();
            tryCleanOutQueue();
        }, 1, 1, TimeUnit.SECONDS);
        // todo delay
    }

    private void retrySendControlPacket(ControlPacketContext cpx) {
        if (!isBound()) {
            log.info("Session({}) retry send packet failed, Session was not bound with a channel", clientIdentifier());
            return;
        }
        if (cpx == null) {
            return;
        }
        // find the first cpx that need retry send
        if (shouldRetrySend(cpx)) {
            ControlPacket retryPacket = cpx.retryPacket();
            log.info("Session({}) retry send Publish({}). retryPacket: {}", clientIdentifier(), cpx, retryPacket);
            doWrite(retryPacket).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    // resend all the packet
                    retrySendControlPacket(cpx.next());
                }
            });
        }
    }

    private boolean shouldRetrySend(ControlPacketContext cpx) {
        if (retryPeriodInMillis == 0) {
            return false;
        }
        return (cpx.elapseAfterLastMark() >= retryPeriodInMillis);
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
        while (sendingPublishThread != null && !channel.eventLoop().inEventLoop(sendingPublishThread)) {
            // spin
        }
        this.eventLoop = channel.eventLoop();
        // better: eventLoop first then channel
        this.channel = channel;
        // try start retry task
        this.eventLoop.submit(() -> {
            // start send some packet
            // send Publish from outQueue immediately
            doSendPublishAndClean();
            // try resend timeout packet in inQueue
            startRetryTask();
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


    public class ControlPacketContextQueue extends AbstractQueue<ControlPacketContext> {

        private final ControlPacketContext head = new ControlPacketContext(null, 0, -1, null);
        private ControlPacketContext tail = head;
        private int size;

        @Override
        public Iterator<ControlPacketContext> iterator() {
            return new Iterator<ControlPacketContext>() {
                ControlPacketContext cur = ControlPacketContextQueue.this.head;
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
                    prev.next(cur.next());
                    size -= 1;
                    cur.next(null);
                    cur = prev;
                    if (size == 0) {
                        tail = head;
                    }
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
            tail.next(cpx);
            tail = cpx;
            size += 1;
            return true;
        }

        @Override
        public ControlPacketContext poll() {
            ControlPacketContext node = head.next();
            if (node != null) {
                head.next(node.next());
                // good for GC
                node.next(null);
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
