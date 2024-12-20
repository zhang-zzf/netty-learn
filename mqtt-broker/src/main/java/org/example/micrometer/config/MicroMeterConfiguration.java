package org.example.micrometer.config;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;

import java.net.InetSocketAddress;

@Slf4j
public class MicroMeterConfiguration {

    public void init(String appName) {
        log.info("MicroMeterConfiguration appName: {}", appName);
        Metrics.globalRegistry.config().commonTags("application", appName);
        String prometheusExport = System.getProperty("prometheus.export.address");
        if (prometheusExport != null) {
            log.info("MicroMeterConfiguration prometheusExport: {}", prometheusExport);
            String[] hostAndPort = prometheusExport.split(":");
            String ipAddress = hostAndPort[0];
            InetSocketAddress address = new InetSocketAddress(ipAddress, Integer.valueOf(hostAndPort[1]));
            InetSocketAddress listened = new PrometheusConfiguration().init(address);
            String serviceId = listened.toString().substring(1).replace(":", "_");
            registerPrometheusToConsul(appName, serviceId, ipAddress, listened.getPort());
        }
        new MicrometerJvmConfiguration().init();
    }

    private void registerPrometheusToConsul(String name, String id,
                                            String address, int port) {
        log.info("MicroMeterConfiguration registerPrometheusToConsul name:{}, id:{}, address:{}, port:{}",
                name, id, address, port);
        String serviceId = (id == null) ? name : id;
        Registration service = ImmutableRegistration.builder()
                .name(name).id(serviceId).address(address).port(port)
                .build();
        log.info("MicroMeterConfiguration registerPrometheusToConsul service: {}", service);
        AgentClient agentClient = Consul.builder()
                .withUrl("http://10.0.9.18:8500")
                .withConnectionPool(new ConnectionPool())
                .build()
                .agentClient();
        agentClient.register(service);
        log.info("service({}/{}) registered to consul", name, id);
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentClient.deregister(serviceId)));
    }

}
