package org.example.mqtt.broker;

import lombok.extern.slf4j.Slf4j;
import org.example.mqtt.model.*;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.example.mqtt.model.ControlPacket.*;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
@Slf4j
public class DefaultServerSession extends AbstractSession implements ServerSession {

    private Broker broker;
    private boolean registered;

    @Override
    public void messageReceived(ControlPacket packet) {
        switch (packet.type()) {
            case SUBSCRIBE:
                doReceiveSubscribe((Subscribe) packet);
                break;
            case UNSUBSCRIBE:
                doReceiveUnsubscribe((Unsubscribe) packet);
                break;
            case DISCONNECT:
                doReceiveDisconnect((Disconnect) packet);
                break;
            default:
                super.messageReceived(packet);
        }
    }

    @Override
    protected void doHandleReceivedPublish(Publish packet) {
        broker.onward(packet);
    }

    private void doReceiveUnsubscribe(Unsubscribe packet) {
        List<Subscription> subscriptions = packet.subscriptions().stream()
                .map(s -> Subscription.from(s.topicFilter(), s.qos(), this))
                .collect(toList());
        broker().deregister(subscriptions);
    }

    private void doReceiveSubscribe(Subscribe packet) {
        List<Subscription> subscriptions = packet.subscriptions().stream()
                .map(s -> Subscription.from(s.topicFilter(), s.qos(), this))
                .collect(toList());
        // register the Subscribe
        List<Subscription> permitted = broker().register(subscriptions);
        List<Subscribe.Subscription> permittedSubscriptions = permitted.stream()
                .map(s -> new Subscribe.Subscription(s.topicFilter(), s.qos()))
                .collect(toList());
        channel().writeAndFlush(SubAck.from(packet.packetIdentifier(), permittedSubscriptions));
    }

    protected void doReceiveDisconnect(Disconnect packet) {
        log.info("receive Disconnect packet, now clean the session and close the Channel");
        this.close();
    }

    @Override
    public Broker broker() {
        return this.broker;
    }

    @Override
    public void register(Broker broker) {
        this.broker = broker;
        this.broker.bind(this);
        this.registered = true;
    }

    @Override
    public boolean isRegistered() {
        return this.registered;
    }

    @Override
    public void close() {
        if (cleanSession()) {
            // disconnect the session from the broker
            broker().disconnect(this);
            this.registered = false;
        }
        super.close();
    }

}
