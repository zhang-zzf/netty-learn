package org.example.mqtt.broker;

import io.netty.channel.Channel;
import org.example.mqtt.broker.jvm.DefaultTopic;
import org.example.mqtt.model.Connect;

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

    @Override
    public Map<Topic.TopicFilter, Subscription> register(Session session,
                                                         List<org.example.mqtt.model.Subscription> subscriptions) {
        Map<Topic.TopicFilter, Subscription> ret = new HashMap<>(subscriptions.size());
        for (org.example.mqtt.model.Subscription sub : subscriptions) {
            Topic.TopicFilter topicFilter = new DefaultTopic.DefaultTopicFilter(sub.getTopic());
            int permittedQoS = decideSubscriptionQos(session, topicFilter, sub.getQos());
            ret.put(topicFilter, new DefaultSubscription(topicFilter, permittedQoS, session));
            Topic topic = topicBy(topicFilter);
            topic.addSubscriber(session, permittedQoS);
        }
        return ret;
    }

    protected abstract Topic topicBy(Topic.TopicFilter topicFilter);

    protected abstract int decideSubscriptionQos(Session session, Topic.TopicFilter topicFilter, int requiredQoS);


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
