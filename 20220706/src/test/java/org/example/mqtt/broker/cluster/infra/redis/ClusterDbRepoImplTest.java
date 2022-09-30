package org.example.mqtt.broker.cluster.infra.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.example.mqtt.broker.cluster.ClusterControlPacketContext;
import org.example.mqtt.broker.cluster.ClusterDbRepo;
import org.example.mqtt.broker.cluster.ClusterServerSession;
import org.example.mqtt.broker.cluster.ClusterTopic;
import org.example.mqtt.model.Publish;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.redisson.api.RedissonClient;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.BDDAssertions.then;
import static org.example.mqtt.session.ControlPacketContext.Status.INIT;
import static org.example.mqtt.session.ControlPacketContext.Type.IN;

@Slf4j
class ClusterDbRepoImplTest {

    final static RedissonClient client;

    static {
        String addresses = "redis://10.255.4.15:7000, redis://10.255.4.15:7001,redis://10.255.4.15:7002";
        client = RedisConfiguration.newRedisson(addresses);
    }

    final static ClusterDbRepo dbRepo = new ClusterDbRepoImpl(client);

    @Test
    void givenTopicFilter_whenConvertToRedisKey_then() {
        ClusterDbRepoImpl impl = new ClusterDbRepoImpl(null);
        then(impl.toTopicFilterRedisKey("topic")).isEqualTo("{topic}");
        then(impl.toTopicFilterRedisKey("topic/ab")).isEqualTo("{topic}/ab");
        // then(impl.convertToRedisKey("topic/ab/")).isEqualTo("{topic}/ab/");
    }

    @ParameterizedTest
    @CsvFileSource(resources = {"/topicFilter/topicFilter.csv"})
    void givenTopicFilter_whenAddNodeToTopic_then(String tf) {
        dbRepo.addNodeToTopic("node01", asList(tf));
        dbRepo.removeNodeFromTopic("node01", asList(tf));
    }

    @Test
    void given_whenAddAndRemove1_then() {
        String topic = "topic/abc/de/mn";
        dbRepo.addNodeToTopic("nod3", asList(topic));
        dbRepo.removeNodeFromTopic("nod3", asList(topic));
        then(dbRepo.matchTopic(topic)).isEmpty();
    }

    @Test
    void given_whenAddAndRemove2_then() {
        String topic1 = "topic/abc/de/mn";
        dbRepo.addNodeToTopic("nod3", asList(topic1));
        String topic2 = "topic/abc";
        dbRepo.addNodeToTopic("nod3", asList(topic2));
        dbRepo.removeNodeFromTopic("nod3", asList(topic1));
        then(dbRepo.matchTopic(topic1)).isEmpty();
        then(dbRepo.matchTopic(topic2)).isNotEmpty();
        dbRepo.removeNodeFromTopic("nod3", asList(topic2));
    }

    @Test
    void given_whenAddAndRemove3_then() {
        String topic1 = "topic/abc/de/mn";
        String topic2 = "topic/abc";
        dbRepo.removeTopic(asList(topic1, topic2));
        dbRepo.addNodeToTopic("nod3", asList(topic1));
        dbRepo.addNodeToTopic("nod3", asList(topic2));
        dbRepo.removeNodeFromTopic("nod3", asList(topic2));
        then(dbRepo.matchTopic(topic2)).isEmpty();
        then(dbRepo.matchTopic(topic1)).isNotEmpty();
        dbRepo.removeNodeFromTopic("nod3", asList(topic1));
    }

    @Test
    @Disabled
    void given_whenPressure_then() {
        List<String> nodes = new ArrayList<>(64);
        for (int i = 0; i < 32; i++) {
            nodes.add("Node" + i);
        }
        // 100W Client
        Set<String> list = new HashSet<>(2024000);
        // example: 021/
        String cIdPrefix = System.getProperty("cId.prefix", "021");
        // 生成 100W id
        backTrace(new StringBuilder(cIdPrefix), 0, list, 6);
        // 线程池
        int maxThread = Runtime.getRuntime().availableProcessors() * 4;
        maxThread = Integer.getInteger("db.pressure.thread.num", maxThread);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4, maxThread,
                60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(maxThread),
                // 提交线程自己跑
                new ThreadPoolExecutor.CallerRunsPolicy());
        if (Boolean.getBoolean("db.pressure.mode.search")) {
            searchPressure(dbRepo, list, threadPool);
        } else {
            indexPressure(dbRepo, nodes, list, threadPool);
        }
    }

    private static void searchPressure(ClusterDbRepo dbRepo,
                                       Set<String> list,
                                       ThreadPoolExecutor threadPool) {
        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        AtomicLong count = new AtomicLong(0);
        AtomicLong respSize = new AtomicLong(0);
        for (String id : list) {
            Runnable task = () -> {
                // String id2 = id.substring(0, id.lastIndexOf("/") + 1) + "+/+";
                List<ClusterTopic> clusterTopics = dbRepo.matchTopic(id);
                respSize.getAndAdd(clusterTopics.size());
                count.getAndIncrement();
                if ((count.get() % 10000) == 0) {
                    long timeUsed = System.currentTimeMillis() - start.get();
                    long qps = 10000 / (timeUsed / 1000);
                    long timePerReq = timeUsed / 10000 * threadPool.getPoolSize();
                    log.info("search 1W request->{}ms/doc, qps:{}, matched: {}", timePerReq, qps, respSize.get());
                    start.set(System.currentTimeMillis());
                    respSize.set(0);
                    count.set(0);
                }
            };
            threadPool.submit(task);
        }
    }

    private static void indexPressure(ClusterDbRepo dbRepo,
                                      List<String> nodes,
                                      Set<String> list,
                                      ThreadPoolExecutor threadPool) {
        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        AtomicLong count = new AtomicLong(0);
        if (Boolean.getBoolean("db.pressure.mode.unsub")) {
            for (String id : list) {
                Runnable task = () -> {
                    dbRepo.removeTopic(Arrays.asList(id));
                    count.getAndIncrement();
                    if ((count.get() % 10000) == 0) {
                        long timeUsed = System.currentTimeMillis() - start.get();
                        long qps = 10000 / (timeUsed / 1000);
                        long timePerReq = timeUsed / 10000 * threadPool.getPoolSize();
                        log.info("index 1W request-> avg: {}ms/doc, qps:{}", timePerReq, qps);
                        start.set(System.currentTimeMillis());
                        count.set(0);
                    }
                };
                threadPool.submit(task);
            }
        } else {
            for (String id : list) {
                Runnable task = () -> {
                    dbRepo.addNodeToTopic(randomNode(nodes), Arrays.asList(id));
                    count.getAndIncrement();
                    if ((count.get() % 10000) == 0) {
                        long timeUsed = System.currentTimeMillis() - start.get();
                        long qps = 10000 / (timeUsed / 1000);
                        long timePerReq = timeUsed / 10000 * threadPool.getPoolSize();
                        log.info("index 1W request-> avg: {}ms/doc, qps:{}", timePerReq, qps);
                        start.set(System.currentTimeMillis());
                        count.set(0);
                    }
                };
                threadPool.submit(task);
            }
        }
    }

    private static void backTrace(StringBuilder buf, int level, Set<String> list, int totalLevel) {
        if (level == totalLevel) {
            list.add(buf.substring(0, buf.length() - 1));
            return;
        }
        for (int i = 0; i < 10; i++) {
            int tLevel = i;
            int length = buf.length();
            buf.append(tLevel).append('/');
            backTrace(buf, level + 1, list, totalLevel);
            buf.delete(length, buf.length());
        }
    }

    @Test
    void givenTopicFilter_whenMatchTopic_then() {
        List<ClusterTopic> resp = dbRepo.matchTopic("0211/2/3/4/5/6");
        then(resp).isNotNull();
    }

    private static String randomNode(List<String> nodes) {
        return nodes.get(new Random().nextInt(32));
    }

    @Test
    void given_whenOfferToSessionQueue_then() {
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) 1, byteBuf);
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                clientIdentifier, IN, packet, INIT, null);
        // when
        // first time success
        boolean firstOffer = dbRepo.offerCpx(null, cpx);
        // then
        then(firstOffer).isTrue();
    }

    @Test
    void given_whenOfferToSessionQueue10Times_then() throws InterruptedException {
        String clientIdentifier = UUID.randomUUID().toString();
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            // when
            // first time success
            boolean firstOffer = dbRepo.offerCpx(null, cpx);
            // then
            then(firstOffer).isTrue();
            Thread.sleep(2);
        }
    }

    @Test
    void given_whenPeekFromSessionQueue_then() throws InterruptedException {
        // given
        // offer 10 cpx
        String clientIdentifier = UUID.randomUUID().toString();
        String payload = "Hello, World!\n你好，世界。";
        ByteBuf byteBuf = Unpooled.copiedBuffer(payload, UTF_8);
        ClusterControlPacketContext tail = null;
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            // when
            // first time success
            boolean offerCpx = dbRepo.offerCpx(tail, cpx);
            // then
            then(offerCpx).isTrue();
            tail = cpx;
            Thread.sleep(2);
        }
        // when
        List<ClusterControlPacketContext> ccpxList = dbRepo.searchCpx(clientIdentifier, IN, false, 1);
        // then
        then(ccpxList.get(0)).isNotNull()
                .returns((short) 0, x -> x.packet().packetIdentifier())
                .returns((short) 1, x -> x.nextPacketIdentifier())
                .returns(clientIdentifier, x -> x.clientIdentifier())
                .returns(payload, x -> x.packet().payload().toString(UTF_8))
                .returns(false, x -> x.packet().retain())
                .returns(2, x -> x.packet().qos())
                .returns(false, x -> x.packet().dup())
                .returns("topic/abc/de", x -> x.packet().topicName())
        ;
        // clean
        for (int i = 0; i < 10; i++) {
            Publish packet = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", (short) i, byteBuf);
            ClusterControlPacketContext cpx = new ClusterControlPacketContext(dbRepo,
                    clientIdentifier, IN, packet, INIT, null);
            // when
            // first time success
            boolean offerCpx = dbRepo.pollCpx(cpx);
            // then
            then(offerCpx).isTrue();
        }
    }

    /**
     */
    @Test
    void given_whenCasUpdateOrCreateIfNotExist_then() {
        String nodeId = UUID.randomUUID().toString();
        // 第一次添加成功
        dbRepo.addNodeToTopic(nodeId, Arrays.asList("topic/abc"));
        // 第二次添加成功（无需更新DB）
        dbRepo.addNodeToTopic(nodeId, Arrays.asList("topic/abc"));
    }

    /**
     * outQueue is empty
     * <p>空队列 追加消息</p>
     */
    @Test
    // todo
    @Disabled
    void givenEmptyOutQueue_whenOfferToOfflineSession_then() {
        // given
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterServerSession session = ClusterServerSession.from(clientIdentifier, null, null, null);
        dbRepo.saveSession(session);
        // use a shadow copy of the origin Publish
        String payload = "Hello, World!\n你好，世界。";
        ByteBuf byteBuf = Unpooled.copiedBuffer(payload, UTF_8);
        short packetIdentifier = session.nextPacketIdentifier();
        Publish outgoing = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", packetIdentifier, byteBuf);
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo, clientIdentifier, IN, outgoing, INIT, null);
        dbRepo.offerToOutQueueOfTheOfflineSession(session, ccpx);
    }

    /**
     * outQueue is empty
     * <p>非空队列 追加消息</p>
     */
    @Test
    // todo
    @Disabled
    void givenNotEmptyOutQueue_whenOfferToOfflineSession_then() {
        // given
        String clientIdentifier = UUID.randomUUID().toString();
        ClusterServerSession session = ClusterServerSession.from(clientIdentifier, null, null, null);
        dbRepo.saveSession(session);
        // 空队列追加
        ClusterControlPacketContext ccpx = newCcpx(clientIdentifier, session.nextPacketIdentifier());
        dbRepo.offerToOutQueueOfTheOfflineSession(session, ccpx);
        // 非空队列追加
        ClusterServerSession dbSession = dbRepo.getSession(clientIdentifier);
        var ccpx2 = newCcpx(clientIdentifier, dbSession.nextPacketIdentifier());
        dbRepo.offerToOutQueueOfTheOfflineSession(dbSession, ccpx2);
    }

    private ClusterControlPacketContext newCcpx(String clientIdentifier, short packetIdentifier) {
        ByteBuf byteBuf = Unpooled.copiedBuffer("Hello, World!\n你好，世界。", UTF_8);
        Publish outgoing = Publish.outgoing(false, (byte) 2, false, "topic/abc/de", packetIdentifier, byteBuf);
        ClusterControlPacketContext ccpx = new ClusterControlPacketContext(dbRepo, clientIdentifier, IN, outgoing, INIT, null);
        return ccpx;
    }

    @Test
    void givenSessionQueue_whenUpdateCpx_then() {
        String notExistClientIdentifier = UUID.randomUUID().toString();
        ClusterControlPacketContext ccpx = newCcpx(notExistClientIdentifier, (short) 1);
        dbRepo.updateCpxStatus(ccpx);
    }

}