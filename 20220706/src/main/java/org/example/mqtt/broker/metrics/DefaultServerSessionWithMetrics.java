package org.example.mqtt.broker.metrics;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.broker.DefaultServerSession;
import org.example.mqtt.model.Publish;
import org.example.mqtt.session.ControlPacketContext;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class DefaultServerSessionWithMetrics extends DefaultServerSession {

    final Timer receive = Timer.builder("com.github.zzf.netty.server.receive")
            .publishPercentileHistogram()
            // 1μs
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(Metrics.globalRegistry);
    final Timer received = Timer.builder("com.github.zzf.netty.server.received")
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(Metrics.globalRegistry);
    final Timer sent = Timer.builder("com.github.zzf.netty.server.sent")
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(Metrics.globalRegistry);

    public DefaultServerSessionWithMetrics(String clientIdentifier) {
        super(clientIdentifier);
    }

    /**
     * 收到 Publish Packet
     */
    @Override
    protected void doReceivePublish(Publish packet) {
        try {
            long millis = packet.payload().getLong(8);
            long now = System.currentTimeMillis();
            // 更新成服务器时间
            packet.payload().setLong(8, now);
            receive.record(now - millis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore
        }
        super.doReceivePublish(packet);
    }

    /**
     * 接受 Publish 完成
     *
     * @param cpx the ControlPacketContext
     */
    @Override
    protected void publishReceived(ControlPacketContext cpx) {
        try {
            long millis = cpx.packet().payload().getLong(8);
            received.record(System.currentTimeMillis() - millis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore
        }
        super.publishReceived(cpx);
    }

    /**
     * 开始处理 Publish
     *
     * @param packet the Publish packet that received from pair
     */
    @Override
    protected boolean onPublish(Publish packet, Future<Void> promise) {
        return super.onPublish(packet, promise);
    }

    @Override
    protected void publishSent(ControlPacketContext cpx) {
        try {
            long millis = cpx.packet().payload().getLong(8);
            sent.record(System.currentTimeMillis() - millis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore
        }
        super.publishSent(cpx);
    }

}