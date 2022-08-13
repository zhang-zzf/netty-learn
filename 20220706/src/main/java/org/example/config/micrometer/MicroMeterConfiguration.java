package org.example.config.micrometer;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import io.micrometer.core.instrument.Metrics;

import java.net.InetSocketAddress;

public class MicroMeterConfiguration {

    public void init(String appName) {
        Metrics.globalRegistry.config().commonTags("application", appName);
        String prometheusExport = System.getProperty("prometheus.export.address");
        if (prometheusExport != null) {
            String[] hostAndPort = prometheusExport.split(":");
            InetSocketAddress address = new InetSocketAddress(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
            InetSocketAddress listened = new PrometheusConfiguration().init(address);
            String serviceId = listened.toString().substring(1).replace(":", "_");
            registerPrometheusToConsul(appName, serviceId, listened.getHostName(), listened.getPort());
        }
        new MicrometerJvmConfiguration().init();
    }

    private void registerPrometheusToConsul(String name, String id,
                                            String address, int port) {
        String serviceId = (id == null) ? name : id;
        Registration service = ImmutableRegistration.builder()
                .name(name).id(serviceId).address(address).port(port)
                .build();
        AgentClient agentClient = Consul.builder().build().agentClient();
        agentClient.register(service);
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentClient.deregister(serviceId)));
    }

}
