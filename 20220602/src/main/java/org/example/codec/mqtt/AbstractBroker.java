package org.example.codec.mqtt;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class AbstractBroker implements Broker {


    @Override
    public void publish(Message message) {
        // route message to relative session
        String messageTopic = message.topic();
        Topic topic = findSubscribeTopic(messageTopic);
        if (topic == null) {
            return;
        }
        for (Subscription sub : topic.subscription()) {
            Message rewrite = rewriteMessage(message, sub);
            sub.session().send(rewrite);
        }
    }

    protected Message rewriteMessage(Message message, Subscription sub) {
        return null;
    }

    protected abstract Topic findSubscribeTopic(String messageTopic);

    @Override
    public void register(Session session) {

    }

    @Override
    public void deregister(Session session) {

    }

}
