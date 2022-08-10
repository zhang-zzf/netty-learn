package com.github.zzf.micrometer.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusConfigurationTest {


    /**
     * 启动 prometheus micro 配置
     */
    @Test
    public void givenPrometheus_whenStart_then() throws InterruptedException {
        PrometheusConfiguration c = new PrometheusConfiguration();
        c.init(9001);
        Thread.currentThread().join();
    }

}