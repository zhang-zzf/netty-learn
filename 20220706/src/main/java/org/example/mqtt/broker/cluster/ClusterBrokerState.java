package org.example.mqtt.broker.cluster;

import lombok.RequiredArgsConstructor;
import org.example.mqtt.broker.BrokerState;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.Topic;
import org.example.mqtt.model.Publish;
import org.example.mqtt.model.Subscribe;

import java.util.List;
import java.util.concurrent.Future;

@RequiredArgsConstructor
public class ClusterBrokerState implements BrokerState {

    final ClusterDbRepo clusterDbRepo;

    @Override
    public ServerSession session(String clientIdentifier) {
        ServerSession session =
                clusterDbRepo.querySessionByClientIdentifier(clientIdentifier);
        return session;
    }

    @Override
    public List<Topic> match(String topicName) {
        return null;
    }

    @Override
    public Future<Void> subscribe(ServerSession session, Subscribe.Subscription subscription) {
        return null;
    }

    @Override
    public Future<Void> unsubscribe(ServerSession session, Subscribe.Subscription subscription) {
        return null;
    }

    @Override
    public Future<Void> disconnect(ServerSession session) {
        return null;
    }

    @Override
    public Future<ServerSession> connect(ServerSession session) {
        return null;
    }

    @Override
    public void removeRetain(Publish packet) {

    }

    @Override
    public void saveRetain(Publish packet) {

    }

    @Override
    public List<Publish> matchRetain(String topicFilter) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

}
