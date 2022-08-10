package com.github.zzf.micrometer.config;

import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Metrics;

public class MicrometerJvmExtrasConfiguration {

    public void init() {
        new ProcessMemoryMetrics().bindTo(Metrics.globalRegistry);
        new ProcessThreadMetrics().bindTo(Metrics.globalRegistry);
    }

}
