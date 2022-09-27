package com.github.zzf.service;

import io.micrometer.core.annotation.Timed;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

@Service
@Timed(histogram = true)
public class SomeService {

    @SneakyThrows
    public String methodA() {
        Thread.sleep(10);
        return "sleep 10 ms";
    }

    @SneakyThrows
    public String methodB() {
        Thread.sleep(12);
        return "sleep 20 ms";
    }

}
