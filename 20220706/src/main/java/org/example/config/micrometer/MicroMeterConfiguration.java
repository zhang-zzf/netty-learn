package org.example.config.micrometer;

import io.micrometer.core.instrument.Metrics;

public class MicroMeterConfiguration {

    public void init(String appName, int exportPort) {
        Metrics.globalRegistry.config().commonTags("application", appName);
        new PrometheusConfiguration().init(exportPort);
        new MicrometerJvmConfiguration().init();
    }

}
