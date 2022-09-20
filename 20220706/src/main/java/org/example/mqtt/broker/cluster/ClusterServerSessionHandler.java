package org.example.mqtt.broker.cluster;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.ServerSession;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.cluster.node.NodeMessage;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;

import static org.example.mqtt.broker.cluster.node.Cluster.$_SYS_NODES_TOPIC;

@Slf4j
public class ClusterServerSessionHandler extends DefaultServerSessionHandler {

    private final Cluster cluster;

    public ClusterServerSessionHandler(Authenticator authenticator,
                                       int activeIdleTimeoutSecond,
                                       Cluster cluster) {
        super(cluster.broker(), authenticator, activeIdleTimeoutSecond);
        this.cluster = cluster;
    }

    @Override
    protected ConnAck doHandleConnect(Connect connect) {
        ConnAck connAck = ConnAck.accepted();
        String ccId = connect.clientIdentifier();
        var preSession = broker().session(ccId);
        log.debug("Client({}) Cluster now has Session: {}", ccId, preSession);
        if (connect.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session", ccId);
            // CleanSession=1 cluster level Session
            if (preSession != null) {
                // close the Session
                closeServerSession(preSession, true);
                log.debug("Client({}) closed the exist preSession", ccId);
            }
            // build a new one (just local Node Session)
            this.session = DefaultServerSession.from(connect);
            log.debug("Client({}) Node created a new Session: {}", ccId, session);
        } else {
            // need a CleanSession=0 cluster level Session
            log.debug("Client({}) need a (cleanSession=0) Session", ccId);
            if (preSession == null) {
                // no Exist Session
                this.session = ClusterServerSession.from(broker().clusterDbRepo(), connect);
                log.debug("Client({}) Cluster created a new Session: {}", ccId, session);
            } else if (preSession instanceof ClusterServerSession) {
                // exist cluster level Session.
                var css = (ClusterServerSession) preSession;
                if (css.isBound()) {
                    log.debug("Client({}) Cluster try to close the old Session", ccId);
                    closeServerSession(css, false);
                    css = (ClusterServerSession) broker().session(ccId);
                    log.debug("Client({}) Cluster closed the old Session", ccId);
                    log.debug("Client({}) Cluster now has Session: {}", ccId, preSession);
                }
                this.session = css.reInitWith(connect);
                connAck = ConnAck.acceptedWithStoredSession();
                log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", ccId, this.session);
            } else if (preSession instanceof DefaultServerSession) {
                // 正常情况下不会出现此流程
                log.warn("Client switch CleanSession 1->0, which should not occur.");
                // preSession is (CleanSession=1) Session
                preSession.close(true);
                // build a new Cluster level Session
                this.session = ClusterServerSession.from(broker().clusterDbRepo(), connect);
                log.debug("Client({}) need a (cleanSession=0) Session, created new Session: {}", ccId, session);
            } else {
                throw new IllegalArgumentException();
            }
        }
        return connAck;
    }

    private void closeServerSession(ServerSession preSession, boolean force) {
        if (preSession instanceof ClusterServerSession) {
            var css = (ClusterServerSession) preSession;
            if (css.isBound()) {
                String nodeId = css.nodeId();
                if (nodeId.equals(broker().nodeId())) {
                    // Session is bound to the Node
                    css.close(force);
                } else {
                    // tell the other Node to close the Session
                    closeServerSessionOnOtherNode(css);
                    if (force) {
                        css.closeClusterSession(true);
                    }
                }
            }
        } else if (preSession instanceof DefaultServerSession) {
            preSession.close(force);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @SneakyThrows
    private void closeServerSessionOnOtherNode(ClusterServerSession css) {
        String sessionNodeId = css.nodeId();
        int count = 0;
        while (true) {
            // Session is bound to a Node of the Cluster
            if (cluster.node(sessionNodeId) == null) {
                // Node was dead.
                break;
            } else {
                NodeMessage nm = NodeMessage.wrapSessionClose(broker().nodeId(), css.clientIdentifier());
                String nodeTargetTopic = $_SYS_NODES_TOPIC + sessionNodeId;
                Publish outgoing = Publish.outgoing(Publish.AT_LEAST_ONCE, nodeTargetTopic, nm.toByteBuf());
                // async notify
                broker().nodeBroker().forward(outgoing);
                // 等待 100 ms
                Thread.sleep(100);
                css = (ClusterServerSession) broker().session(css.clientIdentifier());
                if (!css.isBound()) {
                    break;
                }
            }
            if (++count > 10) {
                throw new IllegalStateException("Session 异常，不知道如何处理了");
            }
        }
    }

    @Override
    protected ClusterBroker broker() {
        return (ClusterBroker) super.broker();
    }

}