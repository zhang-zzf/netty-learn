package org.example.mqtt.broker.cluster;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.example.mqtt.broker.Authenticator;
import org.example.mqtt.broker.cluster.node.Cluster;
import org.example.mqtt.broker.cluster.node.NodeMessage;
import org.example.mqtt.broker.node.DefaultServerSession;
import org.example.mqtt.broker.node.DefaultServerSessionHandler;
import org.example.mqtt.model.ConnAck;
import org.example.mqtt.model.Connect;
import org.example.mqtt.model.Publish;

import static org.example.mqtt.broker.cluster.node.Cluster.nodeListenTopic;

@Slf4j
public class ClusterServerSessionHandler extends DefaultServerSessionHandler {

    private final Cluster cluster;

    public ClusterServerSessionHandler(Authenticator authenticator,
                                       int activeIdleTimeoutSecond,
                                       Cluster cluster) {
        super(cluster.broker(), authenticator, activeIdleTimeoutSecond);
        this.cluster = cluster;
    }

    /**
     * @return DefaultServerSession for cleanSession=1 or ClusterServerSession for cleanSession=0
     */
    @Override
    protected ConnAck doHandleConnect(Connect connect) {
        ConnAck connAck = ConnAck.accepted();
        String ccId = connect.clientIdentifier();
        if (connect.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session", ccId);
            // Just get Session from local Node(Broker)
            var preSession = broker().nodeBroker().session(ccId);
            log.debug("Client({}) Node now has Session: {}", ccId, preSession);
            if (preSession != null) {
                // apply for DefaultServerSession and ClusterServerSession
                preSession.close();
                broker().destroySession(preSession);
                log.debug("Client({}) Node closed the exist preSession", ccId);
            } else {
                // check if there is a Session in the Cluster
                var css = (ClusterServerSession) broker().session(ccId);
                log.debug("Client({}) Cluster now has Session: {}", ccId, css);
                if (css != null) {
                    if (css.isOnline()) {
                        log.info("Client({}) Cluster try to close Online Session(cleanSession=0) on the Node->{}", ccId, css);
                        // online Session on the Node, rare case
                        closeServerSessionOnOtherNode(css);
                    }
                    broker().destroySession(css);
                }
            }
            // build a new one (just local Node Session)
            this.session = broker().createSession(connect);
            log.debug("Client({}) Node created a new Session: {}", ccId, session);
        } else {
            // need a CleanSession=0 cluster level Session
            log.debug("Client({}) need a (cleanSession=0) Session", ccId);
            // Just get Session from local Node(Broker)
            var preSession = broker().nodeBroker().session(ccId);
            log.debug("Client({}) Node now has Session: {}", ccId, preSession);
            if (preSession != null) {
                log.debug("Client({}) Node now try to close Session: {}", ccId, preSession);
                preSession.close();
                if (preSession instanceof ClusterServerSession) {
                    // get Session from the Cluster, it should be set to offline mode
                    var css = (ClusterServerSession) broker().session(ccId);
                    if (css.isOnline()) {
                        throw new IllegalStateException();
                    }
                    log.debug("Client({}) Node now has closed ClusterServerSession: {}", ccId, css);
                    this.session = css.reInitWith(connect);
                    connAck = ConnAck.acceptedWithStoredSession();
                    log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", ccId, this.session);
                } else if (preSession instanceof DefaultServerSession) {
                    this.session = broker().createSession(connect);
                    log.debug("Client({}) Node created a new Session: {}", ccId, session);
                } else {
                    throw new IllegalArgumentException();
                }
            } else {
                // check if there have a Session in the Cluster
                var css = (ClusterServerSession) broker().session(ccId);
                log.debug("Client({}) Cluster now has Session: {}", ccId, css);
                if (css == null) {
                    // no Exist Session
                    this.session = broker().createSession(connect);
                    log.debug("Client({}) Node created a new Session: {}", ccId, session);
                } else {
                    if (css.isOnline()) {
                        log.info("Client({}) Cluster try to close Online Session(cleanSession=0) on the Node->{}", ccId, css);
                        // online Session on the Node, rare case
                        closeServerSessionOnOtherNode(css);
                        // get Session from Cluster again.
                        css = (ClusterServerSession) broker().session(ccId);
                    }
                    // now ClusterSession is offline mode
                    this.session = css.reInitWith(connect);
                    connAck = ConnAck.acceptedWithStoredSession();
                    log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", ccId, this.session);
                }
            }
        }
        return connAck;
    }

    @SneakyThrows
    private void closeServerSessionOnOtherNode(ClusterServerSession css) {
        String sessionNodeId = css.nodeId();
        int count = 0;
        while (true) {
            // Session is bound to a Node of the Cluster
            if (cluster.node(sessionNodeId) == null) {
                log.info("Client({}) Cluster closeServerSessionOnOtherNode, other Node offline ", css.clientIdentifier());
                // Node was dead.
                break;
            } else {
                // 定向 Node 发送消息
                NodeMessage nm = NodeMessage.wrapSessionClose(broker().nodeId(), css.clientIdentifier());
                Publish outgoing = Publish.outgoing(Publish.AT_LEAST_ONCE, nodeListenTopic(sessionNodeId), nm.toByteBuf());
                // async notify
                broker().nodeBroker().forward(outgoing);
                // 等待 100 ms
                Thread.sleep(100);
                css = (ClusterServerSession) broker().session(css.clientIdentifier());
                if (!css.isOnline()) {
                    log.info("Client({}) Cluster closeServerSessionOnOtherNode, ClusterServerSession now is offline", css.clientIdentifier());
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