package org.example.mqtt.session;

import java.util.AbstractQueue;
import java.util.Iterator;

import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;

public class ControlPacketContextQueue extends AbstractQueue<ControlPacketContext> {

    private final ControlPacketContext head = new ControlPacketContext(null, INIT, IN, null);
    private ControlPacketContext tail = head;
    private int size;

    @Override
    public Iterator<ControlPacketContext> iterator() {
        return new Iterator<ControlPacketContext>() {
            ControlPacketContext cur = head;
            ControlPacketContext prev = null;
            boolean nexted = false;

            @Override
            public boolean hasNext() {
                return cur.next() != null;
            }

            @Override
            public ControlPacketContext next() {
                prev = cur;
                cur = cur.next();
                nexted = true;
                return cur;
            }

            @Override
            public void remove() {
                if (!nexted) {
                    throw new IllegalStateException();
                }
                nexted = false;
                prev.next(cur.next());
                size -= 1;
                cur.next(null);
                cur = prev;
                if (size == 0) {
                    tail = head;
                }
            }
        };
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean offer(ControlPacketContext cpx) {
        if (cpx == null) {
            throw new NullPointerException("cpx is null");
        }
        tail.next(cpx);
        tail = cpx;
        size += 1;
        return true;
    }

    @Override
    public ControlPacketContext poll() {
        ControlPacketContext node = head.next();
        if (node != null) {
            head.next(node.next());
            // good for GC
            node.next(null);
            size -= 1;
        }
        if (size == 0) {
            tail = head;
        }
        return node;
    }

    @Override
    public ControlPacketContext peek() {
        return head.next();
    }

}
