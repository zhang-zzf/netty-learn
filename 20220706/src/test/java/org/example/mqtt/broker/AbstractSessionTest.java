package org.example.mqtt.broker;

import org.example.mqtt.session.ControlPacketContext;
import org.example.mqtt.session.ControlPacketContextQueue;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Queue;

import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.OUT;

/**
 * @author zhanfeng.zhang
 * @date 2022/07/03
 */
class AbstractSessionTest {

    @Test
    void givenControlPacketContextQueue_whenOfferAndPoll_then() {
        Queue<ControlPacketContext> queue = new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, INIT, OUT, null));
        then(queue.size()).isEqualTo(1);
        queue.poll();
        then(queue.size()).isEqualTo(0);
        then(queue.poll()).isNull();
        then(queue.size()).isEqualTo(0);
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then() {
        Queue<ControlPacketContext> queue = new ControlPacketContextQueue();
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
        }
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then11() {
        Queue<ControlPacketContext> queue = new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, INIT, OUT, null));
        then(queue.peek()).isNotNull();
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then2() {
        Queue<ControlPacketContext> queue = new ControlPacketContextQueue();
        queue.offer(new ControlPacketContext(null, INIT, OUT, null));
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
        }
    }

    @Test
    void givenControlPacketContextQueue_whenIteration_then3() {
        Queue<ControlPacketContext> queue = new ControlPacketContextQueue();
        ControlPacketContext cpx = new ControlPacketContext(null, INIT, OUT, null);
        queue.offer(cpx);
        queue.offer(cpx);
        Iterator<ControlPacketContext> it = queue.iterator();
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotNull();
            it.remove();
        }
        then(queue.isEmpty()).isTrue();
        // add again
        queue.offer(cpx);
        then(queue.isEmpty()).isFalse();
        int deleteCnt = 0;
        while (it.hasNext()) {
            ControlPacketContext next = it.next();
            then(next).isNotOfAnyClassIn();
            it.remove();
            deleteCnt += 1;
        }
        then(deleteCnt).isEqualTo(1);
    }


}