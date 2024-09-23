package com.github.zzf.micrometer.config;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

@Slf4j
public class PrometheusConfiguration {
    public void init(int port) {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // 添加到 GlobalRegistry
        Metrics.addRegistry(registry);
        // start a httpserver
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = registry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            new Thread(server::start, "prometheus-http-server").start();
            log.info("prometheus exporter start success, port: {}", port);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

    }

}
