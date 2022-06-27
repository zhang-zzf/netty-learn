package org.example.mqtt.broker;

import org.example.mqtt.model.Connect;

/**
 * @author 张占峰 (Email: zhang.zzf@alibaba-inc.com / ID: 235668)
 * @date 2022/6/24
 */
public abstract class AbstractBroker implements Broker {

    @Override
    public Session accepted(Connect packet) throws Exception {
        if (!authenticate(packet)) {
            return null;
        }
        AbstractSession session = findSession(packet.clientIdentifier());
        // clean session is set to 1
        if (packet.cleanSession() && session != null) {
            // discard any previous session if exist
            session.close();
            disconnect(session);
        }
        // clean session is set to 0
        if (!packet.cleanSession() && session != null) {
            return session;
        }
        // create and init session
        session = createNewSession();
        initAndBind(session, packet);
        return session;
    }

    protected abstract AbstractSession findSession(String clientIdentifier);

    /**
     * authenticate the Client Connect
     *
     * @param packet Connect
     * @return true / false
     */
    protected abstract boolean authenticate(Connect packet);

    protected abstract AbstractSession createNewSession();

    protected void initAndBind(AbstractSession session, Connect packet) {
        session.clientIdentifier(packet.clientIdentifier());
        session.keepAlive(packet.keepAlive());
        session.persistent(packet.cleanSession());
        // session bind to broker;
        bind(session);
    }

    protected abstract void bind(AbstractSession session);

}
