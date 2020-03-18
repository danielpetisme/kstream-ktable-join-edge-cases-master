package com.sample.kafka;

import kafka.server.KafkaConfig;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EmbeddedSingleNodeKafkaCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);

    private static final short OFFSETS_TOPIC_REPLICATION_FACTOR = 1; // Defaults to 3, override to 1 since we run a single broker

    private ZooKeeperEmbedded zookeeper;
    private KafkaBrokerEmbedded broker;
    public final String bootstrapServers;
    private AdminClient adminClient;

    public EmbeddedSingleNodeKafkaCluster() throws Exception {
        this(Collections.emptyMap());
    }

    public EmbeddedSingleNodeKafkaCluster(Map<String, Object> config) throws Exception {
        this.zookeeper = new ZooKeeperEmbedded(InstanceSpec.getRandomPort());
        this.broker = new KafkaBrokerEmbedded(brokerConfig(config));
        this.bootstrapServers = this.broker.listener;
    }

    private Map<String, Object> brokerConfig(Map<String, Object> config) {
        var brokerConfig = new HashMap<String, Object>();
        brokerConfig.put(KafkaConfig.PortProp(), InstanceSpec.getRandomPort());
        brokerConfig.put(KafkaConfig.ZkConnectProp(), zookeeper.connectString());
        brokerConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), OFFSETS_TOPIC_REPLICATION_FACTOR);
        brokerConfig.putAll(config);
        return brokerConfig;
    }

    public void start() throws Exception {
        zookeeper.start();
        log.info("Zookeeper started...");
        broker.start();
        log.info("Kafka broker started...");
        adminClient = KafkaAdminClient.create(Map.of("bootstrap.servers", bootstrapServers));
        log.info("Kafka admin client created...");

    }

    public void stop() throws Exception {
        broker.stop();
        log.info("Zookeeper stopped...");
        zookeeper.stop();
        log.info("Kafka broker stopped...");
    }

    public void createTopic(String topic) {
        createTopic(topic, 1, 1);
    }


    public void createTopic(String topic, int numPartitions, int replicationFactor) {
        createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
    }

    public void createTopic(String topic, int numPartitions, int replicationFactor, Map<String, String> topicConfig) {
        log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }", topic, numPartitions, replicationFactor, topicConfig);
        var newTopic = new NewTopic(topic, numPartitions, (short) replicationFactor);
        newTopic.configs(topicConfig);
        adminClient.createTopics(List.of(newTopic));
    }

    public void deleteTopic(String topic) {
        adminClient.deleteTopics(List.of(topic));
    }

}
