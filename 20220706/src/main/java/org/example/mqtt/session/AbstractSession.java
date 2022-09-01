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

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static org.example.mqtt.model.ControlPacket.*;
import static org.example.mqtt.model.Publish.AT_LEAST_ONCE;
import static org.example.mqtt.model.Publish.EXACTLY_ONCE;
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

    /**
     * Close the Channel that was used to connect to the client.
     */
    @Override
    public void closeChannel() {
        if (retryTask != null) {
            retryTask.cancel(true);
            retryTask = null;
        }
        if (isBound()) {
            if (channel.isOpen()) {
                channel.close();
            }
            channel = null;
        }
        for (ControlPacketContext cpx : outQueue) {
            if (cpx.inSending()) {
                // remark the SENDING cpx
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
        log.debug("send: {}, {}", clientIdentifier(), packet);
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
            log.info("Session is not bind to a Channel, discard the Publish: {}, {}", cId(), outgoing);
            promise.tryFailure(new IllegalStateException("Client is Down"));
            return;
        }
        // very little chance
        if (qos2DuplicateCheck(outgoing, outQueue)) {
            promise.trySuccess(null);
            log.info("Session({}) send same qos2 packet: {}", cId(), outgoing);
            return;
        }
        // enqueue
        ControlPacketContext cpx = new ControlPacketContext(outgoing, INIT, OUT, promise);
        outQueue.offer(cpx);
        log.debug("sendPublish({}) .->INIT [outQueue 入队]: {}, {}", cpx.pId(), cId(), cpx);
        // start send some packet
        doSendPublishAndClean();
    }

    protected String cId() {
        return clientIdentifier();
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
        log.debug("sendPublish({}) INIT->SENDING: {}, {}", cpxToUse.pId(), cId(), cpxToUse);
        // there is no concurrency problem, but the doWrite is async progress.
        // send data and add send complete listener
        doWrite(cpxToUse.packet()).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                cpxToUse.markStatus(SENDING, SENT);
                log.debug("sendPublish({}) SENDING->SENT: {}, {}", cpxToUse.pId(), cId(), cpxToUse);
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
            if (next.packet().atMostOnce()) {
                // clean all QoS 0 cpx
                it.remove();
                publishSent(next);
            } else if (controlPacketTimeout(next)) {
                // clean all timeout cpx
                it.remove();
                log.warn("Session({}) clean timeout Publish({}) in outQueue after {}ms.", cId(), next, next.elapseAfterLastMark());
                publishSendTimeout(next);
            }
        }
    }

    protected void publishSendTimeout(ControlPacketContext next) {
        next.timeout();
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
            Publish packet = next.packet();
            if ((packet.atMostOnce()) || (packet.atLeastOnce())) {
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
        log.debug("receivePublish({}) received [消息接受完成，从 inQueue 中移除]: {}, {}", cpx.pId(), cId(), cpx);
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
        log.debug("sendPublish({}) sent [消息发送完成，从 outQueue 中移除]: {}, {}", cpx.pId(), cId(), cpx);
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
        log.debug("receivePubComp: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findCpx(outQueue, EXACTLY_ONCE, pId);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubComp nothing
            log.error("Session({}) PubComp nothing. {}, queue: {}", clientIdentifier(), pId, outQueue);
            return;
        }
        cpx.markStatus(PUB_REC, PUB_COMP);
        log.debug("sendPublish({}) PUB_REC->PUB_COMP [QoS2 收到 Client PUB_COMP]: {}, {}", packet.pId(), cId(), packet);
        // try clean the queue
        tryCleanOutQueue();
    }

    private ControlPacketContext findCpx(Queue<ControlPacketContext> queue, int qos, short packageIdentifier) {
        for (ControlPacketContext cpx : queue) {
            if (cpx.packet().qos() == qos && cpx.packet().packetIdentifier() == packageIdentifier) {
                return cpx;
            }
        }
        return null;
    }

    /**
     * as Sender
     */
    private void doReceivePubRec(PubRec packet) {
        log.debug("receivePubRec: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findCpx(outQueue, EXACTLY_ONCE, pId);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRec nothing
            log.error("Session({}) PubRec nothing. {}, queue: {}", cId(), pId, outQueue);
            return;
        }
        cpx.markStatus(SENT, PUB_REC);
        log.debug("sendPublish({}) SENT->PUB_REC [QoS2 收到 Client PUB_REC]: {}, {}", packet.pId(), cId(), cpx);
        // send PubRel packet.
        doWrite(cpx.pubRel()).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("sendPublish({}) PUB_REC->. [QoS2 发送 PUB_REL 给 Client]: {}, {}", packet.pId(), cId(), cpx);
            }
        });
        // no need clean the queue
    }

    /**
     * as Sender
     */
    private void doReceivePubAck(PubAck packet) {
        log.debug("receivePubAck: {}, {}", cId(), packet);
        short pId = packet.packetIdentifier();
        // only look for the first QoS 1 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findCpx(outQueue, AT_LEAST_ONCE, pId);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {
            // Client PubAck nothing
            log.error("Session({}) PubAck nothing. {}, queue: {}", clientIdentifier(), pId, outQueue);
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
        // only look for the first QoS 2 ControlPacketContext that match the PacketIdentifier
        ControlPacketContext cpx = findCpx(inQueue, EXACTLY_ONCE, pId);
        // now cpx point to the first QoS 2 ControlPacketContext or null
        if (cpx == null) {
            // Client PubRel nothing
            log.error("Session({}) PubRel nothing. {}, queue: {}", cId(), pId, inQueue);
            return;
        }
        cpx.markStatus(HANDLED, PUB_REL);
        log.debug("receivePublish({}) HANDLED->PUB_REL [QoS2 收到 Client PUB_REL]: {}, {}", cpx.pId(), cId(), cpx);
        // ack PubComp to Client
        doWrite(cpx.pubComp()).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                cpx.markStatus(PUB_REL, PUB_COMP);
                log.debug("receivePublish({}) PUB_REL->PUB_COMP [QoS2 已发送 PUB_COMP 给 Client]: {}, {}", cpx.pId(), cId(), cpx);
                tryCleanInQueue();
            }
        });
    }

    /**
     * as Receiver
     */
    protected void doReceivePublish(Publish packet) {
        log.debug("receivePublish({}): {}, {}", packet.pId(), cId(), packet);
        if (packet.needAck() && existSamePacket(packet, inQueue)) {
            log.warn("Session({}) receive same Publish packet: {}", clientIdentifier(), packet.packetIdentifier());
            return;
        }
        Promise<Void> receiveProgress = newPromise();
        ControlPacketContext cpx = new ControlPacketContext(packet, INIT, IN, receiveProgress);
        log.debug("receivePublish({}) .->INIT [inQueue 入队]: {}, {}", cpx.pId(), cId(), cpx);
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
        log.debug("receivePublish({}) INIT->HANDLED: {}, {}", cpx.pId(), cId(), cpx);
        // now cpx is HANDLED
        if (packet.atMostOnce()) {
            tryCleanInQueue();
        } else if (packet.atLeastOnce()) {
            doWrite(cpx.pubAck()).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    cpx.markStatus(HANDLED, PUB_ACK);
                    log.debug("receivePublish({}) HANDLED->PUB_ACK [QoS1 消息已处理且已发送 PUB_ACK 给 Client]: {}, {}", cpx.pId(), cId(), cpx);
                    tryCleanInQueue();
                }
            });
        } else if (packet.exactlyOnce()) {
            // does not modify the status of the cpx
            doWrite(cpx.pubRec()).addListener(f -> {
                if (f.isSuccess()) {
                    log.debug("receivePublish({}) HANDLED ->. [QoS2 消息已处理且已发送 PUB_REC 给 Client]: {}, {}", cpx.pId(), cId(), cpx);
                }
            });
        }
    }

    protected DefaultPromise<Void> newPromise() {
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
                .addListener(FIRE_EXCEPTION_ON_FAILURE)
                .addListener(LOG_ON_FAILURE)
                ;
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
                    log.debug("Session({}) retry send Publish({}). retryPacket: {}", clientIdentifier(), cpx, retryPacket);
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
            // start send some packet.
            // send Publish from outQueue immediately.
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


}
