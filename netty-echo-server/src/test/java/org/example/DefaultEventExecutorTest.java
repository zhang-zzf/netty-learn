package org.example;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class DefaultEventExecutorTest {

    DefaultEventExecutorGroup executors = new DefaultEventExecutorGroup(4);

    @SneakyThrows
    @Test
    void given_when_then() {
        asyncGet("Hello, World")
                .addListener(f -> log.info("result -> {}", f.getNow()))
                .sync();
    }

    Future<String> asyncGet(String key) {
        EventExecutor executor = executors.next();
        Promise<String> future = executor.newPromise();
        executor.submit(() -> {
            try {
                Thread.sleep(1000);
                future.setSuccess(key);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return future;
    }
}
