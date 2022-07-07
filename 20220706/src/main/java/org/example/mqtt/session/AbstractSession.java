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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.mqtt.model.ControlPacket.*;
import static org.example.mqtt.model.Publish.EXACTLY_ONCE;
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
    private final AtomicInteger pocketIdentifier = new AtomicInteger(0);
    private boolean cleanSession;

    private Queue<ControlPacketContext> inQueue = new ControlPacketContextQueue();
    private Queue<ControlPacketContext> outQueue = new ControlPacketContextQueue();

    private ScheduledFuture retryTask;
    private final int retryPeriodInMillis = 0;

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

    @Override
    public boolean cleanSession() {
        return cleanSession;
    }

    @Override
    public Future<Void> send(ControlPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        return sendInEventLoop(packet);
    }

    protected Promise<Void> sendInEventLoop(ControlPacket packet) {
        if (this.eventLoop == null) {
            throw new IllegalStateException();
        }
        DefaultPromise<Void> promise = newPromise();
        // make sure use the safe thread that the session wad bound to
        if (eventLoop.inEventLoop()) {
            invokeSend(packet, promise);
        } else {
            eventLoop.execute(() -> invokeSend(packet, promise));
        }
        return promise;
    }

    private void invokeSend(ControlPacket packet, Promise promise) {
        // send immediately if can or queue the packet
        // put Publish packet into queue
        if (PUBLISH == packet.type()) {
            doSendPublish((Publish) packet, promise);
        } else {
            doSendPacket(packet, promise);
        }
    }

    private void doSendPacket(ControlPacket packet, Promise promise) {
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

    private void doSendPublish(Publish outgoing, Promise promise) {
        // very little chance
        if (qos2DuplicateCheck(outgoing, outQueue)) {
            promise.trySuccess(null);
            return;
        }
        // enqueue
        outQueue.offer(new ControlPacketContext(outgoing, ControlPacketContext.CREATED, OUT, promise));
        // start send some packet
        doSendPublishPacket(outQueue.peek());
        tryCleanQueue(outQueue);
    }

    private boolean qos2DuplicateCheck(Publish packet, Queue<ControlPacketContext> queue) {
        if (packet.exactlyOnce() && existSamePacket(packet, queue)) {
            log.info("Session({}) qos2 same packet: {}", clientIdentifier(), packet);
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
        // clean all timeout cpx
        Iterator<ControlPacketContext> timeoutIt = queue.iterator();
        while (timeoutIt.hasNext()) {
            ControlPacketContext next = timeoutIt.next();
            if (controlPacketTimeout(next)) {
                log.warn("Session({}) clean timeout Publish", clientIdentifier());
                timeoutIt.remove();
                publishSendFailed(next);
            }
        }
        // clean all QoS 0 cpx
        Iterator<ControlPacketContext> qos0It = queue.iterator();
        while (qos0It.hasNext()) {
            ControlPacketContext next = qos0It.next();
            if (!next.packet().needAck()) {
                qos0It.remove();
                publishSendSuccess(next);
            }
        }
        // just clean complete cpx from head
        ControlPacketContext cpx = queue.peek();
        // cpx always point to the first cpx in the queue
        while (cpx != null && cpx.complete()) {
            queue.poll();
            publishSendSuccess(cpx);
            cpx = queue.peek();
        }
    }

    protected void publishSendFailed(ControlPacketContext cpx) {
        log.info("Session({}) send Publish failed. {}", clientIdentifier(), cpx);
        cpx.getPromise().tryFailure(new TimeoutException("cpx timeout"));
    }

    private boolean controlPacketTimeout(ControlPacketContext cpx) {
        int timeoutMillis = retryPeriodInMillis > 0 ? 2 * retryPeriodInMillis : 3000;
        return (System.currentTimeMillis() - cpx.getMarkedMillis() >= timeoutMillis);
    }

    /**
     * invoke after send Publish successfully.
     *
     * @param cpx Publish
     */
    protected void publishSendSuccess(ControlPacketContext cpx) {
        cpx.getPromise().trySuccess(null);
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
            log.warn("Session({}) send same Publish packet: {}", clientIdentifier(), packet.packetIdentifier());
            return;
        }
        Promise<Void> promise = newPromise();
        ControlPacketContext cpx = new ControlPacketContext(packet, RECEIVED, IN, promise);
        inQueue.offer(cpx);
        publishReceived(packet, promise);
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

    private DefaultPromise<Void> newPromise() {
        return new DefaultPromise<>(this.eventLoop);
    }

    /**
     * do handle the Publish from the pair
     *
     * @param packet the Publish packet that received from pair
     */
    protected abstract void publishReceived(Publish packet, Future<Void> promise);

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
            boolean emptyQueue = inQueue.isEmpty() && outQueue.isEmpty();
            if (!isBound() || emptyQueue) {
                // cancel the scheduled task
                this.retryTask.cancel(false);
                this.retryTask = null;
                return;
            }
            retrySendControlPacket(inQueue.peek());
            retrySendControlPacket(outQueue.peek());
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void retrySendControlPacket(ControlPacketContext cpx) {
        if (!isBound()) {
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

    private boolean shouldRetrySend(ControlPacketContext qosPacket) {
        if (retryPeriodInMillis == 0) {
            return false;
        }
        return (System.currentTimeMillis() - qosPacket.getMarkedMillis() >= retryPeriodInMillis);
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
                    prev.setNext(cur.next());
                    size -= 1;
                    cur.setNext(null);
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
