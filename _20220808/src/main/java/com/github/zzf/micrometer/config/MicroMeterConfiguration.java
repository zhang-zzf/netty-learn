package com.github.zzf.micrometer.config;

import io.micrometer.core.instrument.Metrics;

public class MicroMeterConfiguration {


    public void config() {
        Metrics.globalRegistry.config().commonTags("application", "_20220808");
        new PrometheusConfiguration().init(9001);
        new MicrometerJvmExtrasConfiguration().init();
    }

}
