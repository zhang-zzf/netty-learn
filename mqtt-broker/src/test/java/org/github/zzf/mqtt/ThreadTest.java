package org.github.zzf.mqtt;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * @author : zhanfeng.zhang@icloud.com
 * @date : 2026-01-30
 */
@Slf4j
public class ThreadTest {


    @Test
    void given_when_then() {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        List<Person> thread1List = new LinkedList<>();
        Person p = new Person("a");
        executor.submit(() -> {
            thread1List.add(p);
        });
        // p.setName("b");
        executor.submit(() -> {
            for (Person o : thread1List) {
                // 此处看到的 name 是 "a" 还是 "b"
                log.info("{}", o);
            }
        });
    }

    static class Person {
        private String name;

        public Person(String name) {
            this.name = name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
