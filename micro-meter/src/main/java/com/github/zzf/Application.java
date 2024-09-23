package com.github.zzf;

import com.github.zzf.micrometer.config.MicroMeterConfiguration;
import com.github.zzf.service.SomeService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhang.zzf
 * @date 2020-04-18
 */
@Slf4j
@Configuration
@ComponentScan(basePackageClasses = {
        Application.class
})
public class Application {

    public static void main(String[] args) {
        log.info("starting app");
        //
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);
        // 启动打点
        new MicroMeterConfiguration().init();
        // Timer
        Timer timer = Timer.builder("com.github.zzf.timer.random")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.8, 0.9, 0.95, 0.99)
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(10))
                .register(Metrics.globalRegistry);
        Runnable timerTask = () -> timer.record(new Random().nextInt(100), TimeUnit.MILLISECONDS);
        // 1 ms 更新一次
        executorService.scheduleAtFixedRate(timerTask, 100, 1, TimeUnit.MILLISECONDS);
        // Timer2
        Timer timer2 = Metrics.timer("com.github.zzf.timer.random", "type", "randomTest2");
        Runnable timerTask2 = () -> timer2.record(new Random().nextInt(100), TimeUnit.MILLISECONDS);
        // 1 ms 更新一次
        executorService.scheduleAtFixedRate(timerTask2, 100, 10, TimeUnit.MILLISECONDS);
        // counter
        Counter counter = Metrics.counter("com.github.zzf.counter.random");
        Runnable counterTask = () -> counter.increment(new Random().nextInt(100));
        // 1 s 更新一次
        executorService.scheduleAtFixedRate(counterTask, 1, 1, TimeUnit.SECONDS);
        // Gauge
        AtomicLong gauge = Metrics.gauge("com.github.zzf.gauge", new AtomicLong());
        Runnable gaugeTask = () -> gauge.set(new Random().nextInt(100));
        // 10 s 更新一次
        executorService.scheduleAtFixedRate(gaugeTask, 1, 10, TimeUnit.SECONDS);
        // 启动 springContext
        ApplicationContext context = new AnnotationConfigApplicationContext(Application.class);
        SomeService someService = context.getBean(SomeService.class);
        executorService.scheduleWithFixedDelay(() -> someService.methodA(), 1, 10, TimeUnit.SECONDS);
        executorService.scheduleWithFixedDelay(() -> someService.methodB(), 1, 10, TimeUnit.SECONDS);
    }

}
