package org.example.mqtt.broker;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Queue;

import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.broker.ControlPacketContext.OUT;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/03
 */
class AbstractSessionTest {

    @Test
    void givenControlPacketContextQueue_whenOfferAndPoll_then() {
        Queue<ControlPacketContext> queue = new DefaultServerSession(null).new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, 0, OUT));
        then(queue.size()).isEqualTo(1);
        queue.poll();
        then(queue.size()).isEqualTo(0);
        then(queue.poll()).isNull();
        then(queue.size()).isEqualTo(0);
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then() {
        Queue<ControlPacketContext> queue = new DefaultServerSession(null).new ControlPacketContextQueue();
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
        }
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then11() {
        Queue<ControlPacketContext> queue = new DefaultServerSession(null).new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, 1, OUT));
        then(queue.peek()).isNotNull();
    }


    @Test
    void givenControlPacketContextQueue_whenIteration_then2() {
        Queue<ControlPacketContext> queue = new DefaultServerSession(null).new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, 0, OUT));
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
        }
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then3() {
        Queue<ControlPacketContext> queue = new DefaultServerSession(null).new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, 0, OUT));
        queue.offer(new ControlPacketContext(null, 0, OUT));
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
            it.remove();
        }
        then(queue.isEmpty()).isTrue();
    }





}