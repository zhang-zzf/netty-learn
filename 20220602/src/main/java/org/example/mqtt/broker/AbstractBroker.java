package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.broker.jvm.DefaultTopic;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Subscribe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class AbstractBroker implements Broker {

    protected abstract List<Topic> topicBy(String topicName);

    @Override
    public Session accepted(Connect packet, Channel channel) throws Exception {
        AbstractSession session = findSession(packet.clientIdentifier());
        // clean session is set to 0
        if (!packet.cleanSession() && session != null) {
            return session;
        }
        // clean session is set to 1
        if (packet.cleanSession() && session != null) {
            // discard any previous session if exist
            session.close();
            disconnect(session);
        }
        // create and init session
        session = createNewSession(channel);
        initAndBind(session, packet);
        return session;
    }

    protected abstract Topic topicBy(Topic.TopicFilter topicFilter);

    protected abstract int decideSubscriptionQos(Subscription subscription);


    /**
     * find session that bound to the clientIdentifier
     *
     * @param clientIdentifier ClientIdentifier
     * @return the session if exists or null
     */
    protected abstract AbstractSession findSession(String clientIdentifier);

    /**
     * authenticate the Client Connect
     *
     * @param packet Connect
     * @return true / false
     */
    protected abstract boolean authenticate(Connect packet);

    protected abstract AbstractSession createNewSession(Channel channel);

    protected void initAndBind(AbstractSession session, Connect packet) {
        session.clientIdentifier(packet.clientIdentifier());
        session.cleanSession(packet.cleanSession());
        // session bind to broker;
        bindSession(session);
    }

    protected abstract void bindSession(AbstractSession session);

}
