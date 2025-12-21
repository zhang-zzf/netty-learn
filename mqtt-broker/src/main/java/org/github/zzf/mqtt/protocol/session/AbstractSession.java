package org.github.zzf.mqtt.protocol.session;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBACK;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBCOMP;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBLISH;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBREC;
import static org.github.zzf.mqtt.protocol.model.ControlPacket.PUBREL;
import static org.github.zzf.mqtt.protocol.model.Publish.AT_LEAST_ONCE;
import static org.github.zzf.mqtt.protocol.model.Publish.AT_MOST_ONCE;
import static org.github.zzf.mqtt.protocol.model.Publish.EXACTLY_ONCE;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.HANDLED;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.INIT;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.PUB_ACK;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.PUB_COMP;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.PUB_REC;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Status.PUB_REL;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Type.IN;
import static org.github.zzf.mqtt.protocol.session.ControlPacketContext.Type.OUT;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.model.PubAck;
import org.github.zzf.mqtt.protocol.model.PubComp;
import org.github.zzf.mqtt.protocol.model.PubRec;
import org.github.zzf.mqtt.protocol.model.PubRel;
import org.github.zzf.mqtt.protocol.model.Publish;

/**
 * @author zhanfeng.zhang@icloud.com
 * @date 2024-11-05
 */
@Slf4j
// todo QoS1 / QoS 2 重试发送
public abstract class AbstractSession implements Session {

    public static final ChannelFutureListener LOG_ON_FAILURE = future -> {
        if (!future.isSuccess()) {
            log.error("Channel({}).writeAndFlush failed.", future.channel(), future.cause());
        }
    };

    protected final AtomicInteger packetIdentifier = new AtomicInteger(new Random().nextInt(Short.MAX_VALUE));

    private final String clientIdentifier;
    private final boolean cleanSession;

    private final Channel channel;

    /**
     * 是否发送 Publish Packet
     */
    // private volatile Thread sendingPublishThread;
    protected AbstractSession(String clientIdentifier, boolean cleanSession, Channel channel) {
        this.clientIdentifier = checkNotNull(clientIdentifier, "clientIdentifier");
        this.cleanSession = cleanSession;
        this.channel = checkNotNull(channel);
    }

    @Override
    public boolean cleanSession() {
        return cleanSession;
    }

    /**
     * <pre>
     *     how to define promise is success
     *      1. Publish promise is always success
     *          1. QoS0 promise is success
     *          1. QoS1 / QoS2 进入 outQueue 队列成功 -> promise is success
     *      1. other ControlPacket promise is success only when the packets was sent to peer.
     * </pre>
     */
    @Override
    public ChannelFuture send(ControlPacket packet) {
        log.debug("sender({}): . -> {}", cId(), packet);
        if (packet == null) {
            throw new IllegalArgumentException();
        }
        // 测试 channel 关闭后是否可以正常使用 Promise ?
        // 已测试，channel 关闭后 channel.newPromise() / channel.eventLoop() 可以正常使用
        final ChannelPromise promise = channel.newPromise();
        if (packet.type() == PUBLISH) {
            doInEventLoop(() -> doSendPublish((Publish) packet, promise));
        }
        else {// send ControlPacket other than Publish
            doInEventLoop(() -> doSendControlPacketExceptPublish(packet, promise));
        }
        return promise;
    }

    private void doSendControlPacketExceptPublish(ControlPacket packet, ChannelPromise promise) {
        if (!isActive()) {
            log.warn("sender({}): . -> DISCARDED, Session is not bind to a Channel, discard the ControlPacket", cId());
            promise.setFailure(new IllegalStateException("Session is not bind to a Channel"));
            return;
        }
        doWrite(packet).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("sender({}): . -> SENT", cId());
                promise.setSuccess();
            }
            else {
                promise.setFailure(f.cause());
            }
        });
    }

    private void doInEventLoop(Runnable task) {
        // channel.eventLoop() exists even after channel was closed
        // make sure use the same thread that the session wad bound to
        if (channel.eventLoop().inEventLoop()) {
            task.run();
        }
        else {
            channel.eventLoop().execute(task);
        }
    }

    private void doSendPublish(Publish packet, final ChannelPromise promise) {
        // Session 使用 EventLoop 更改内部状态
        assert channel.eventLoop().inEventLoop();
        log.debug("sender({}/{}) Publish . -> INIT: {}", cId(), packet.pId(), packet);
        switch (packet.qos()) {
            case AT_MOST_ONCE:
                doSendAtMostOncePublish(packet, promise);
                break;
            case AT_LEAST_ONCE:
                doSendAtLeastOncePublish(packet, promise);
                break;
            case EXACTLY_ONCE:
                doSendExactlyOncePublish(packet, promise);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void doSendAtMostOncePublish(Publish packet, ChannelPromise promise) {
        if (isActive()) {// online, send immediately
            // qos0 比 qos1/qos2 更早发送。。？ OK (match the mqtt protocol)
            channel.writeAndFlush(packet).addListener(f -> {
                if (f.isSuccess()) {
                    publishSent(packet);
                }
                // no matter success of failed.
                publishSentComplete(packet);
            });
        }
        else {// offline. consider success.
            publishSentComplete(packet);
        }
        // anyway the result is success (match the QoS rules)
        promise.setSuccess();
    }

    protected boolean isActive() {
        return channel.isActive();
    }

    private void doSendAtLeastOncePublish(Publish packet, ChannelPromise promise) {
        enqueueOutQueueAndSend(packet);
        // anyway the result is success (match the QoS rules)
        promise.setSuccess();
    }

    private void doSendExactlyOncePublish(Publish packet, ChannelPromise promise) {
        // check duplicated, very little chance
        if (outQueueQos2DuplicateCheck(packet)) {
            log.warn("sender({}/{}) Publish INIT -> DUPLICATED", cId(), packet.pId());
            promise.setSuccess();
            return;
        }
        enqueueOutQueueAndSend(packet);
        // anyway the result is success (match the QoS rules)
        promise.setSuccess();
    }

    private void enqueueOutQueueAndSend(Publish packet) {
        // enqueue
        Queue<ControlPacketContext> outQueue = outQueue();
        outQueue().offer(createNewCpx(packet, INIT, OUT));
        log.debug("sender({}/{}) Publish INIT -> ENQUEUE", cId(), packet.pId());
        if (!isActive()) {
            return;
        }
        // try to send the packet
        if (inFlightWindowEquals(1)) {
            ControlPacketContext cpx = outQueue.peek();
            if (cpx == null || cpx.packetIdentifier() != packet.packetIdentifier()) {
                return;
            }
            doHandleSendPublish(cpx);
        }
        else {
            for (ControlPacketContext cpx : outQueue) {// 批量发送
                doHandleSendPublish(cpx);
            }
        }
    }

    private void doHandleSendPublish(ControlPacketContext cpx) {
        if (cpx == null || cpx.outgoingPublishSent()) {
            return;
        }
        doWrite(cpx.packet()).addListener(f -> {
            if (f.isSuccess()) {
                cpx.markStatus(INIT, HANDLED);
                publishSent(cpx.packet());
            }
        });
    }

    private boolean headOfQueue(short pId, Queue<ControlPacketContext> queue) {
        ControlPacketContext head = queue.peek();
        return head != null && head.packetIdentifier() == pId;
    }

    protected String cId() {
        return clientIdentifier();
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
            log.debug("sender({}/{}) Publish * -> DEQUEUE", cId(), cpx.pId());
            publishSentComplete(cpx.packet());
            cpx = outQueue.peek();
        }
    }

    /**
     * 消息接受完成
     *
     * @param packet the ControlPacketContext
     */
    protected void publishReceivedComplete(Publish packet) {
        log.debug("receiver({}/{}) Publish * -> COMPLETED", cId(), packet.pId());
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
        log.debug("sender({}/{}) Publish PUB_REC -> [receive PUB_COMP]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {// Client PubComp nothing
            log.error("sender({}/{}) PubComp failed, No Publish in outQueue", cId(), packet.pId());
            return;
        }
        // mark status
        cpx.markStatus(PUB_REC, PUB_COMP);
        log.debug("sender({}/{}) Publish PUB_REC -> PUB_COMP", cId(), packet.pId());
        if (headOfQueue(pId, outQueue())) {
            tryCleanOutQueue();
            // now send the next item in the queue
            doHandleSendPublish(outQueue().peek());
        }
    }

    private boolean inFlightWindowEquals(int windowSize) {
        return Integer.getInteger("mqtt.in.flight.window", 1) == windowSize;
    }

    /**
     * as Sender
     */
    private void doReceivePubRec(PubRec packet) {
        log.debug("sender({}/{}) Publish HANDLED -> [receive PubRec]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        if (cpx == null) {
            // Client PubRec nothing
            log.error("sender({}/{}) PubRec failed. No Publish in outQueue", cId(), packet.pId());
            return;
        }
        cpx.markStatus(HANDLED, PUB_REC);
        log.debug("sender({}/{}) Publish HANDLE -> PUB_REC", cId(), packet.pId());
        // send PubRel packet.
        doWritePubRelPacket(cpx);
    }

    private void doWritePubRelPacket(ControlPacketContext cpx) {
        doWrite(new PubRel(cpx.packetIdentifier())).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("sender({}/{}) Publish PUB_REC -> [PubRel sent]", cId(), cpx.pId());
            }
        });
    }

    /**
     * as Sender
     */
    private void doReceivePubAck(PubAck packet) {
        log.debug("sender({}/{}) Publish SENT -> [receive PUB_ACK]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInOutQueue(pId);
        // now cpx point to the first QoS 1 ControlPacketContext or null
        if (cpx == null) {// Client PubAck nothing
            log.error("sender({}/{}) PubAck failed, No Publish in outQueue", cId(), packet.pId());
            return;
        }
        // mark status
        cpx.markStatus(HANDLED, PUB_ACK);
        log.debug("sender({}/{}) Publish SENT -> PUB_ACK", cId(), packet.pId());
        // clean the out queue and send the next Publish packet
        if (headOfQueue(pId, outQueue())) {
            // try clean the queue
            tryCleanOutQueue();
            // now send the next item in the queue
            doHandleSendPublish(outQueue().peek());
        }
    }

    /**
     * as Receiver
     */
    private void doReceivePubRel(PubRel packet) {
        log.debug("receiver({}/{}) Publish HANDLED -> [receive PubRel]", cId(), packet.pId());
        short pId = packet.packetIdentifier();
        ControlPacketContext cpx = findControlPacketInInQueue(pId);
        if (cpx == null) {// PubRel nothing
            log.error("receiver({}/{}) PubRel failed, No Publish in inQueue", cId(), packet.pId());
            return;
        }
        cpx.markStatus(HANDLED, PUB_REL);
        log.debug("receiver({}/{}) Publish HANDLED -> PUB_REL", cId(), cpx.pId());
        // ack PubComp
        doWrite(new PubComp(packet.packetIdentifier())).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("receiver({}/{}) Publish PUB_REL -> [PUB_COMP sent]", cId(), cpx.pId());
                cpx.markStatus(PUB_REL, PUB_COMP);
                log.debug("receiver({}/{}) Publish PUB_REL -> PUB_COMP", cId(), cpx.pId());
                // just clean complete cpx from head
                Queue<ControlPacketContext> inQueue = inQueue();
                // cpx always point to the first cpx in the queue
                ControlPacketContext header = inQueue.peek();
                while (header != null && header.complete()) {
                    inQueue.poll();
                    log.debug("receiver({}/{}) Publish PUB_COMP -> DEQUEUE", cId(), header.pId());
                    publishReceivedComplete(header.packet());
                    header = inQueue.peek();
                }
            }
        });
    }

    /**
     * as Receiver
     */
    protected void doReceivePublish(Publish packet) {
        debugPacketInfo(packet);
        switch (packet.qos()) {
            case AT_MOST_ONCE:
                doReceiveAtMostOncePublish(packet);
                break;
            case AT_LEAST_ONCE:
                doReceiveAtLeastOncePublish(packet);
                break;
            case EXACTLY_ONCE:
                doReceiveExactlyOncePublish(packet);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void doReceiveAtMostOncePublish(Publish packet) {
        // handle the Publish Packet
        onPublish(packet);
        log.debug("receiver({}/{}) Publish INIT -> HANDLED", cId(), packet.pId());
        publishReceivedComplete(packet);
    }

    private void debugPacketInfo(Publish packet) {
        if (log.isDebugEnabled()) {
            log.debug("receiver({}/{}) Publish . -> RECEIVED : {}\n{}", cId(), packet.pId(), packet,
                ByteBufUtil.prettyHexDump(packet.payload()));
        }
    }

    private void doReceiveAtLeastOncePublish(Publish packet) {
        // handle the Publish Packet
        onPublish(packet);
        log.debug("receiver({}/{}) Publish INIT -> HANDLED", cId(), packet.pId());
        doWrite(new PubAck(packet.packetIdentifier())).addListener(f -> {
            log.debug("receiver({}/{}) Publish HANDLED -> [PUB_ACK sent]", cId(), packet.pId());
            publishReceivedComplete(packet);
        });
    }

    private void doReceiveExactlyOncePublish(Publish packet) {
        // duplicate check
        ControlPacketContext dupCpx = findControlPacketInInQueue(packet.packetIdentifier());
        if (dupCpx != null) {
            log.warn("Session({}) receive same Publish(QoS2) packet: {}, {}", cId(), packet.pId(), dupCpx);
            doHandleDuplicateQoS2Publish(dupCpx);
            return;
        }
        // Client / Broker must be online.
        ControlPacketContext cpx = createNewCpx(packet, INIT, IN);
        log.debug("receiver({}/{}) Publish . -> INIT", cId(), packet.pId());
        // enqueue
        inQueue().offer(cpx);
        log.debug("receiver({}/{}) Publish INIT -> ENQUEUE", cId(), packet.pId());
        // handle the Publish Packet
        onPublish(packet);
        // now cpx is HANDLED
        cpx.markStatus(INIT, HANDLED);
        log.debug("receiver({}/{}) Publish ENQUEUE -> HANDLED", cId(), packet.pId());
        // does not modify the status of the cpx
        doWrite(new PubRec(packet.packetIdentifier()))
            .addListener(f -> log.debug("receiver({}/{}) Publish HANDLED -> [PUB_REC sent]", cId(), packet.pId()));
    }

    protected ControlPacketContext createNewCpx(Publish packet,
        ControlPacketContext.Status status,
        ControlPacketContext.Type type) {
        return new ControlPacketContext(packet, status, type);
    }

    private void doHandleDuplicateQoS2Publish(ControlPacketContext cpx) {
        switch (cpx.status()) {
            case INIT:
                log.debug("receiver({}/{}) Publish INIT -> . [QoS2 重复消息，inQueue 队列中状态为 INIT]: {}", cId(), cpx.pId(), cpx);
                break;
            case HANDLED:
                // does not modify the status of the cpx
                doWrite(new PubRec(cpx.packet().packetIdentifier())).addListener(f -> {
                    if (f.isSuccess()) {
                        log.debug("receiver({}/{}) HANDLED -> . [PUB_REC sent]: {}", cId(), cpx.pId(), cpx);
                    }
                });
                break;
            default:
                throw new IllegalStateException();
        }
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
            // todo 测试 异常如何处理？，所有 Listener 都会被执行？
            // 当前实现是直接关闭 channel
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
            doHandleSendPublish(cpx);
        }
        else if (packet.exactlyOnce()) {
            switch (cpx.status()) {
                case INIT -> doHandleSendPublish(cpx);
                // send PubRel packet.
                case PUB_REC -> doWritePubRelPacket(cpx);
                default -> throw new IllegalStateException();
            }
        }
    }

    /**
     * invoke after publish packet sent to receiver
     */
    protected void publishSent(Publish packet) {
        log.debug("sender({}/{}) Publish * -> SENT payload.refCnt: {}", cId(), packet.pId(), packet.payload().refCnt());
    }

    /**
     * invoke after Publish packet sent complete
     *
     * @param packet Publish
     */
    protected void publishSentComplete(Publish packet) {
        int refCnt = packet.payload().refCnt();
        log.debug("sender({}/{}) Publish * -> COMPLETED payload.refCnt: {}", cId(), packet.pId(), refCnt);
        if (refCnt != 0) {// memory leak
            throw new IllegalStateException();
        }
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
