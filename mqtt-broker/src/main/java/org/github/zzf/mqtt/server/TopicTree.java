package org.github.zzf.mqtt.server;


import static org.github.zzf.mqtt.protocol.model.ControlPacket.splitSlashSeparateStr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.github.zzf.mqtt.protocol.model.ControlPacket;
import org.github.zzf.mqtt.protocol.server.Topic;

/**
 * topicFilter as path
 */
@Slf4j
public class TopicTree extends SlashTree<Topic> implements AutoCloseable {

    public TopicTree(String name) {
        super(name);
    }

    static final String MULTI_LEVEL_WILDCARD = ControlPacket.MULTI_LEVEL_WILDCARD;
    static final String SINGLE_LEVEL_WILDCARD = ControlPacket.SINGLE_LEVEL_WILDCARD;
    static final String $ = ControlPacket.DOLLAR;

    /**
     * 以 topicName 找匹配的 Topic
     */
    @Override
    public List<Topic> match(String topicName) {
        List<Node<Topic>> ret = new ArrayList<>(2);
        dfsMatch(splitSlashSeparateStr(topicName), 0, root, ret);
        Stream<Node<Topic>> stream;
        // The Server MUST NOT match Topic Filters starting with a wildcard character (# or +) with Topic Names beginning with a $ character
        if (topicName.startsWith($)) {
            stream = ret.stream().filter(this::dollarMatch);
        }
        else {
            stream = ret.stream();
        }
        return stream
                .map(n -> n.data.get())
                .filter(Objects::nonNull)
                .toList()
                ;
    }

    private boolean dollarMatch(Node<Topic> t) {
        return !t.path.startsWith(MULTI_LEVEL_WILDCARD)
                && !t.path.startsWith(SINGLE_LEVEL_WILDCARD);
    }

    private void dfsMatch(String[] topicLevels,
            int levelIdx,
            Node<Topic> cur,
            List<Node<Topic>> ret) {
        Node<Topic> n;
        if (levelIdx == topicLevels.length) {
            addNodeToList(ret, cur);
            if ((n = cur.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
                addNodeToList(ret, n);
            }
            return;
        }
        String topicLevel = topicLevels[levelIdx];
        if ((n = cur.childNodes.get(topicLevel)) != null) {
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
        if ((n = cur.childNodes.get(MULTI_LEVEL_WILDCARD)) != null) {
            addNodeToList(ret, n);
        }
        if ((n = cur.childNodes.get(SINGLE_LEVEL_WILDCARD)) != null) {
            dfsMatch(topicLevels, levelIdx + 1, n, ret);
        }
    }

    private void addNodeToList(List<Node<Topic>> ret,
            Node<Topic> node) {
        if (node.path != null) {
            ret.add(node);
        }
    }

}
