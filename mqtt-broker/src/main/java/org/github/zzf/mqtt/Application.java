package org.github.zzf.mqtt;

import java.net.URISyntaxException;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.server.TopicBlocker;
import org.github.zzf.mqtt.server.BrokerBootstrap;
import org.github.zzf.mqtt.server.DefaultRoutingTable;
import org.github.zzf.mqtt.server.DefaultTopicBlocker;
import org.github.zzf.mqtt.server.TopicTreeRetain;
import org.github.zzf.mqtt.server.metric.MicroMeterMetrics;

@Slf4j
public class Application {

    public static void main(String[] args) throws URISyntaxException, SSLException {
        //
        int workerThreadNum = Integer.getInteger("mqtt.server.thread.num",
                Runtime.getRuntime().availableProcessors() * 2);
        log.info("MQTT_SERVER_WORKER_THREAD_NUM-> {}", workerThreadNum);
        //
        String serverListenedAddress = System.getProperty("mqtt.server.listened.address",
                "mqtt://0.0.0.0:1883,mqtts://0.0.0.0:8883,ws://0.0.0.0:80,wss://0.0.0.0:443");
        log.info("mqtt.server.listened: {}", serverListenedAddress);
        //
        BrokerBootstrap brokerBootstrap = BrokerBootstrap.builder()
                .authenticator(packet -> 0x00)
                .routingTable(new DefaultRoutingTable())
                .retainPublishManager(new TopicTreeRetain("RetainPublishManager"))
                .topicBlocker(topicBlocker())
                .workerThreadNum(workerThreadNum)
                .serverListenedAddress(serverListenedAddress)
                .activeIdleTimeoutSecond(Integer.getInteger("mqtt.server.active.idle.timeout.second", 3))
                .build()
                .start();
        // metric
        String appName = System.getProperty("appName", "mqtt-broker");
        log.info("appName: {}", appName);
        MicroMeterMetrics.builder().appName(appName).build().init();
    }

    private static TopicBlocker topicBlocker() {
        return new DefaultTopicBlocker() {{
            String tfConfig = System.getProperty("mqtt.server.block.tf", "+/server/#");
            log.info("mqtt.server.block.tf: {}", tfConfig);
            add(tfConfig.split(","));
        }};
    }

}
