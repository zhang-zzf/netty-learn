package org.github.zzf.mqtt.mqtt.broker.cluster;

import io.netty.channel.ChannelHandlerContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.mqtt.broker.cluster.node.Cluster;
import org.github.zzf.mqtt.mqtt.broker.cluster.node.NodeMessage;
import org.github.zzf.mqtt.server.DefaultServerSessionHandler;
import org.github.zzf.mqtt.protocol.model.Publish;


@Slf4j
public class ClusterServerSessionHandler extends DefaultServerSessionHandler {

    private final Cluster cluster;

    public ClusterServerSessionHandler(int activeIdleTimeoutSecond, Cluster cluster) {
        super(cluster.broker(), activeIdleTimeoutSecond);
        this.cluster = cluster;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // todo
        // if (cluster.broker().closed()) {
        //     log.info("Broker was closed, reject new Channel->{}", ctx.channel());
        //     ctx.close();
        //     return;
        // }
        super.channelActive(ctx);
    }

    /**
     * @return DefaultServerSession for cleanSession=1 or ClusterServerSession for cleanSession=0
    @Override
    protected ConnAck doHandleConnect(Connect connect, ChannelHandlerContext ctx) {
        ConnAck connAck = ConnAck.accepted();
        String ccId = connect.clientIdentifier();
        if (connect.cleanSession()) {
            log.debug("Client({}) need a (cleanSession=1) Session", ccId);
            // Just get Session from local Node(Broker)
            var preSession = broker().nodeBroker().session(ccId);
            log.debug("Client({}) Node now has Session: {}", ccId, preSession);
            if (preSession != null) {// 当前节点存在 ClusterServerSession (NodeServerSession / ClusterServerSessionImpl)
                // apply for ClusterServerSession (NodeServerSession / ClusterServerSessionImpl)
                // todo
                // preSession.close(); // will detach session from broker
                log.debug("Client({}) Node closed the exist preSession", ccId);
            }
            else {// 当前节点无 ClusterServerSession
                // check if there is a Session in the Cluster
                var css = (ClusterServerSession) broker().session(ccId);
                log.debug("Client({}) Cluster now has Session: {}", ccId, css);
                if (css != null) {
                    if (css.isOnline()) {
                        log.info("Client({}) Cluster try to close Online Session(cleanSession=0) on the Node -> {}", ccId, css);
                        // online Session on the Node, rare case
                        closeServerSessionOnOtherNode(css);
                    }
                    // delete the cluster Session anyway;
                    // todo
                    // broker().state().deleteSession(css);
                    broker().detachSession(css, true);
                }
            }
            // create a new one (just local Node Session)
            broker().attachSession(this.session = new ClusterServerSessionCleanImpl(connect, ctx.channel(), broker()));
            log.debug("Client({}) Node created a new Session: {}", ccId, session);
        }
        else {// need a CleanSession=0 cluster level Session
            log.debug("Client({}) need a (cleanSession=0) Session", ccId);
            // Just get Session from local Node(Broker)
            var preSession = broker().nodeBroker().session(ccId);
            log.debug("Client({}) Node now has Session: {}", ccId, preSession);
            if (preSession != null) {
                log.debug("Client({}) Node now try to close Session: {}", ccId, preSession);
                // todo
                // preSession.close();
                if (preSession instanceof ClusterServerSessionCleanImpl cssci) { // cleanSession
                    broker().attachSession(this.session = new ClusterServerSessionImpl(connect, ctx.channel(), broker()));
                    log.debug("Client({}) Node created a new Session: {}", ccId, session);
                }
                else if (preSession instanceof ClusterServerSessionImpl) {
                    // get Session from the Cluster, it should be set to offline mode
                    var css = (ClusterServerSession) broker().session(ccId);
                    if (css.isOnline()) {
                        throw new IllegalStateException();
                    }
                    log.debug("Client({}) Node now has closed ClusterServerSession: {}", ccId, css);
                    broker().attachSession(this.session = new ClusterServerSessionImpl(connect, ctx.channel(), broker()).migrate(css));
                    connAck = ConnAck.acceptedWithStoredSession();
                    log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", ccId, this.session);
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
            else { // 当前节点无 ClusterServerSession
                // check if there have a Session in the Cluster
                var css = (ClusterServerSession) broker().session(ccId);
                log.debug("Client({}) Cluster now has Session: {}", ccId, css);
                if (css == null) {
                    // no Exist Session
                    broker().attachSession(this.session = new ClusterServerSessionImpl(connect, ctx.channel(), broker()));
                    log.debug("Client({}) Node created a new Session: {}", ccId, session);
                }
                else {
                    if (css.isOnline()) {
                        log.info("Client({}) Cluster try to close Online Session(cleanSession=0) on the Node->{}", ccId, css);
                        // online Session on the Node, rare case
                        closeServerSessionOnOtherNode(css);
                        // get Session from Cluster again.
                        css = (ClusterServerSession) broker().session(ccId);
                    }
                    // now ClusterSession is offline mode
                    broker().attachSession(this.session = new ClusterServerSessionImpl(connect, ctx.channel(), broker()).migrate(css));
                    connAck = ConnAck.acceptedWithStoredSession();
                    log.debug("Client({}) need a (cleanSession=0) Session, use exist Session: {}", ccId, this.session);
                }
            }
        }
        return connAck;
    }
     */

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
            }
            else {
                // 定向 Node 发送消息
                String topicName = cluster.pickOneChannelToNode(sessionNodeId);
                NodeMessage nm = NodeMessage.wrapSessionClose(broker().nodeId(), css.clientIdentifier());
                Publish outgoing = Publish.outgoing(Publish.AT_LEAST_ONCE, topicName, nm.toByteBuf());
                // async notify
                // todo broker().forward
                broker().nodeBroker().forward(outgoing);
                // todo 优化
                // 等待 100 ms
                Thread.sleep(100);
                // todo
                // css = (ClusterServerSession) broker().session(css.clientIdentifier());
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