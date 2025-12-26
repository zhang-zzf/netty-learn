package org.github.zzf.mqtt.server.metric;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.sun.net.httpserver.HttpServer;
import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;

@Slf4j
@Builder
public class MicroMeterMetrics {

    final String appName;

    public void init() {
        log.info("MicroMeterConfiguration appName: {}", appName);
        Metrics.globalRegistry.config().commonTags("application", appName);
        /* 192.168.0.12:0 */
        String prometheusExport = System.getProperty("prometheus.export.address");
        if (prometheusExport != null) {
            log.info("MicroMeterConfiguration prometheusExport: {}", prometheusExport);
            InetSocketAddress listened = initPrometheusExporter(prometheusExport);
            /* "http://10.0.9.18:8500" */
            String consulAgentAddress = System.getProperty("consul.agent.address");
            if (consulAgentAddress != null) {
                registerPrometheusToConsul(listened, consulAgentAddress);
            }
        }
        initMetrics();
    }

    private InetSocketAddress initPrometheusExporter(String exportAddress) {
        String[] hostAndPort = exportAddress.split(":");
        InetSocketAddress address = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // 添加到 GlobalRegistry
        Metrics.addRegistry(registry);
        // start a httpserver
        try {
            HttpServer server = HttpServer.create(address, 0);
            server.createContext("/metrics", httpExchange -> {
                String response = registry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            Thread thread = new Thread(server::start, "prometheus-http-server");
            thread.setDaemon(true);
            thread.start();
            InetSocketAddress listenedAddress = server.getAddress();
            log.info("prometheus exporter start success, bound: {}", listenedAddress);
            return listenedAddress;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void initMetrics() {
        MeterRegistry registry = Metrics.globalRegistry;
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessMemoryMetrics().bindTo(registry);
        new ProcessThreadMetrics().bindTo(registry);
    }

    private void registerPrometheusToConsul(
            InetSocketAddress listened,
            String consulAgentAddress) {
        String id = listened.toString().substring(1).replace(":", "_");
        String address = listened.getHostString();
        int port = listened.getPort();
        log.info("MicroMeterConfiguration registerPrometheusToConsul name:{}, id:{}, address:{}, port:{}",
                appName, id, address, port);
        Registration service = ImmutableRegistration.builder()
                .name(appName).id(id).address(address).port(port)
                .build();
        log.info("MicroMeterConfiguration registerPrometheusToConsul service: {}", service);
        AgentClient agentClient = Consul.builder()
                .withUrl(consulAgentAddress)
                .withConnectionPool(new ConnectionPool())
                .build()
                .agentClient();
        agentClient.register(service);
        log.info("service({}/{}) registered to consul", appName, id);
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentClient.deregister(id)));
    }

}
