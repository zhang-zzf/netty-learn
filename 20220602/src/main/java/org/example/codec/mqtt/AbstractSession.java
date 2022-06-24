package org.example.codec.mqtt;

import io.netty.util.concurrent.ScheduledFuture;

import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.codec.mqtt.ProgressMessage.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class AbstractSession implements Session {

    private final AtomicInteger pocketIdentifier = new AtomicInteger(0);
    private volatile boolean waitForClientAck = false;
    private volatile ScheduledFuture sendMessageRetryTask;

    private final int retryPeriod = 3000;

    @Override
    public void send(Message message) {
        // init packet identifier
        message.resetPocketIdentifier(nextPocketIdentifier());
        Queue<Message> out = outQueue();
        if (!outQueue().isEmpty() || messageNeedClientAck(message)) {
            out.offer(message);
            sendLater();
        } else {
            doSend((ProgressMessage) message);
        }
    }

    protected abstract Queue<Message> outQueue();

    protected void sendLater() {
        if (waitForClientAck) {
            return;
        }
        channel().eventLoop().submit(() -> {
            Queue<Message> out = outQueue();
            while (!out.isEmpty()) {
                ProgressMessage message = (ProgressMessage) out.peek();
                if (!messageNeedClientAck(message)) {
                    doSend(message);
                    // delete the message
                    out.poll();
                    continue;
                }
                if (message.progress() == STATUS_INIT) {
                    waitForClientAck = true;
                    doSend(message);
                    startSendMessageRetryTask();
                    // wait for ack
                    break;
                }
            }
        });
    }

    protected void startSendMessageRetryTask() {
        if (waitForClientAck && sendMessageRetryTask == null) {
            this.sendMessageRetryTask = channel().eventLoop().scheduleWithFixedDelay(() -> {
                if (!waitForClientAck) {
                    // no longer wait for client acknowledge
                    // cancel the scheduled task;
                    sendMessageRetryTask.cancel(true);
                    sendMessageRetryTask = null;
                    return;
                }
                Queue<Message> outQueue = outQueue();
                ProgressMessage message = (ProgressMessage) outQueue.peek();
                if (messageSendFailed(message)) {
                    if (message.progress() == STATUS_SENT) {
                        // re send message;
                        doSend(message);
                    } else if (message.progress() == STATUS_RELEASE) {
                        doSendPubRelease(message);
                    }
                }
            }, 1, 1, TimeUnit.SECONDS);
        }
    }

    private boolean messageSendFailed(ProgressMessage message) {
        return System.currentTimeMillis() - message.lastProgressTime() > retryPeriod;
    }

    protected void doSendPubRelease(ProgressMessage message) {
        int packetIdentifier = message.packetIdentifier();
        // convert to PUBREL
        // todo
        message.updateProgress(STATUS_RELEASE);
    }

    private boolean messageNeedClientAck(Message message) {
        return message.qos() > 0;
    }

    protected void doSend(ProgressMessage message) {
        channel().writeAndFlush(message);
        message.updateProgress(STATUS_SENT);
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
    public void receive(Message message) {

    }

}
