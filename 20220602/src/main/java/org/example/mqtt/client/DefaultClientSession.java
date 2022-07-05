package org.example.mqtt.client;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.AbstractSession;
import org.example.mqtt.broker.Subscription;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.ControlPacket;
import org.example.mqtt.model.Publish;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/7/5
 */
@Slf4j
public class DefaultClientSession extends AbstractSession implements ClientSession {

    private final Lock lock = new ReentrantLock();
    private final Condition serverResponse = lock.newCondition();
    private volatile ControlPacket response;

    public DefaultClientSession(String clientIdentifier) {
        super(clientIdentifier);
    }

    @Override
    protected void doHandleReceivedPublish(Publish packet) {

    }

    @Override
    public List<Subscription> subscriptions() {
        return null;
    }

    @Override
    public boolean syncConnect(Connect connect) {
        // 超时时间
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

    private boolean receiveConAck() {
        return (response instanceof ConnAck);
    }

}
