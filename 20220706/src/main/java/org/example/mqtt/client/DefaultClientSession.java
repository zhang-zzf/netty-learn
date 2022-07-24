package org.example.mqtt.client;

import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.session.AbstractSession;
import org.example.mqtt.model.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.example.mqtt.model.ControlPacket.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
@Slf4j
public class DefaultClientSession extends AbstractSession implements ClientSession {

    private final Lock lock = new ReentrantLock();
    private final Condition serverResponse = lock.newCondition();
    private volatile ControlPacket response;

    private Set<Subscribe.Subscription> subscriptions = new HashSet<>();

    private final Client client;

    public DefaultClientSession(Client client) {
        super(client.clientIdentifier());
        this.client = client;
    }


    @Override
    public void messageReceived(ControlPacket packet) {
        switch (packet.type()) {
            case CONNACK:
            case SUBACK:
            case UNSUBACK:
                doNotify(packet);
                break;
            default:
                super.messageReceived(packet);
        }
    }

    @Override
    protected boolean onPublish(Publish packet, Future<Void> promise) {
        promise.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                client.messageReceived(packet);
            } else {
                // 有待确定
                throw new RuntimeException(future.cause());
            }
        });
        return true;
    }

    @Override
    public Set<Subscribe.Subscription> subscriptions() {
        return subscriptions;
    }

    @Override
    public boolean syncConnect(Connect connect) {
        send(connect);
        boolean requireExpected = doWait(3000L, cp -> cp instanceof ConnAck);
        if (!requireExpected) {
            log.warn("syncConnect expected ConnAck, actual: {}", this.response);
            return false;
        }
        ConnAck connAck = (ConnAck) this.response;
        if (!connAck.connectionAccepted()) {
            log.warn("syncConnect server refuse Connect: {}", connAck);
            return false;
        }
        return true;
    }

    @Override
    public boolean syncSubscribe(Subscribe subscribe) {
        subscribe.packetIdentifier(nextPacketIdentifier());
        send(subscribe);
        boolean requireExpected = doWait(3000L, cp -> cp instanceof SubAck);
        if (!requireExpected) {
            return false;
        }
        SubAck subAck = (SubAck) this.response;
        if (subAck.packetIdentifier() != subAck.packetIdentifier()) {
            return false;
        }
        List<Subscribe.Subscription> request = subscribe.subscriptions();
        for (int i = 0; i < request.size(); i++) {
            Subscribe.Subscription sub = request.get(i);
            sub.qos(subAck.subscriptions().get(i).qos());
            this.subscriptions.add(sub);
        }
        return true;
    }

    @Override
    public boolean syncUnSubscribe(Unsubscribe unsubscribe) {
        unsubscribe.packetIdentifier(nextPacketIdentifier());
        send(unsubscribe);
        boolean requireExpected = doWait(3000L, cp -> cp instanceof UnsubAck);
        if (!requireExpected) {
            return false;
        }
        UnsubAck unsubAck = (UnsubAck) this.response;
        if (unsubAck.packetIdentifier() != unsubscribe.packetIdentifier()) {
            return false;
        }
        return true;
    }

    @Override
    public boolean syncSend(Publish publish) {
        publish.packetIdentifier(nextPacketIdentifier());
        try {
            send(publish).sync();
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    private boolean doWait(long timeoutMillis, Predicate<ControlPacket> condition) {
        lock.lock();
        try {
            this.response = null;
            long currentTimeMillis = System.currentTimeMillis();
            while (!condition.test(this.response)) {
                if (!serverResponse.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    break;
                }
                timeoutMillis -= (System.currentTimeMillis() - currentTimeMillis);
            }
        } catch (InterruptedException e) {
            log.warn("waitForServerResponse was interrupted");
        } finally {
            lock.unlock();
        }
        return condition.test(this.response);
    }

    private void doNotify(ControlPacket packet) {
        lock.lock();
        try {
            this.response = packet;
            serverResponse.signal();
        } finally {
            lock.unlock();
        }
    }

}
