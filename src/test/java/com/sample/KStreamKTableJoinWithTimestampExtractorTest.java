package com.sample;

import com.sample.kafka.EmbeddedSingleNodeKafkaCluster;
import com.sample.kafka.IntegrationTestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KStreamKTableJoinWithTimestampExtractorTest {

    public static final Logger log = LoggerFactory.getLogger(KStreamKTableJoinWithTimestampExtractorTest.class.getName());

    public static final Serde<String> stringSerde = Serdes.String();

    private static final String userClicksTopic = "user-clicks";
    private static final String userRegionsTopic = "user-regions";
    private static final String outputTopic = "output-topic";
    private static final String intermediateTopic = "intermediate-topic";

    private static EmbeddedSingleNodeKafkaCluster cluster;

    @TempDir
    File stateDir;

    @BeforeEach
    public void setup() throws Exception {
        cluster = new EmbeddedSingleNodeKafkaCluster();
        cluster.start();
        cluster.createTopic(userClicksTopic);
        cluster.createTopic(userRegionsTopic);
        cluster.createTopic(intermediateTopic);
    }

    @AfterEach
    public void tearsDown() throws Exception {
        cluster.stop();
    }


    @Test
    @Order(1)
    public void theoryIsCool() throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));


        userClicksStream
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();
        log.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromktable")
                ),
                producerConfig
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "300|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "200|fromstream")
                ),
                producerConfig
        );

        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        // Since we use a timestamp extraxtor, the stream join on the key + a functional timestamp
        // For repeatability, Kafka ensures the join to be done on a coherent Ktable changelog state.
        // Here the Stream event with timestamp "200" will join with a Ktable state < timestamp "200" ie. "100"
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice", "200|fromstream100|fromktable")
        ));

        streams.close();
    }

    @Test
    @Order(2)
    public void givenNullValueThenShouldNotJoin() throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new TimestampExtractor() {
            private TimestampExtractor myTimestampExtractor = new MyTimestampExtractor();

            @Override
            public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                if (record.value() == null) {
                    return 100; // since the "MyTimestampExtractor" can't compute a timestamp for a null record, we force the value 100
                }
                return myTimestampExtractor.extract(record, partitionTime);
            }
        }));

        userClicksStream
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();

        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", null)
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "200|fromstream")
                ),
                producerConfig
        );

        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,

                        0
                );
        // According to the KStream-KTable documentation
        // https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#kstream-ktable-join
        // > Input records for the table with a null value are interpreted as tombstones for the corresponding key,
        // > which indicate the deletion of the key from the table. Tombstones do not trigger the join.
        assertThat(actualClicksPerRegion.size()).isEqualTo(0);
        streams.close();
    }

    @Test
    @Order(3)
    public void givenSelectKeyShouldJoinOnLatestValue() throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));

        userClicksStream
                .selectKey((key, value) -> key) // We introduce a dummy select key operation
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "300|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "200|fromstream")
                ),
                producerConfig
        );

        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        // We would expect the same behavior as the "theoryIsCool" and is not...
        // The topology includes a "selectKey" operation which implies an intermediate topic, a repartition and the creation of a new sub-topology
        // As for now, Kafka Streams create a StreamTask per sub-topology (so 2 in our context) and the 2 stream tasks are not applying the same timestamp synchro.
        // As result, the join is made with the latest KTable value despite a functional timestamp extractor --> it's a KafkaStream Bug reported to Confluent
        // The solution is to manually split the pipeline by creating an intermediate physical topic
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice", "200|fromstream300|fromktable")
        ));
        streams.close();
    }

    @Test
    @Order(3)
    public void givenSelectKeyWithSubTopologiesShouldJoinOnLatestValue() throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));


        userClicksStream
                .selectKey((key, value) -> key)
                .through(intermediateTopic) // internally the `.through()` is a syntaxic suggar for `.to(my-topic); return builder.stream("my-topic")`
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "300|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "200|fromstream")
                ),
                producerConfig
        );


        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        // the `.through()` operation is not enough and does not fix the `givenSelectKeyShouldJoinOnLatestValue` test behavior
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice", "200|fromstream300|fromktable")
        ));
        streams.close();
    }

    @Test
    @Order(4)
    public void givenSelectKeyAndTwoTopologiesShouldJoinOnGoodValue() throws Exception {
        // We now explicitly declare 2 separated topologies
        Properties streamsConfiguration1 = new Properties();
        streamsConfiguration1.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt() + "-1");
        streamsConfiguration1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration1.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration1.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder1 = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder1.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        userClicksStream
                .selectKey((key, value) -> key)
                .to(intermediateTopic, Produced.with(stringSerde, stringSerde));
        Topology topology1 = builder1.build();
        log.info("Topology1: \n" + topology1.describe());
        KafkaStreams streams1 = new KafkaStreams(topology1, streamsConfiguration1);

        Properties streamsConfiguration2 = new Properties();
        streamsConfiguration2.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt() + "-2");
        streamsConfiguration2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration2.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration2.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder2 = new StreamsBuilder();
        KStream<String, String> intermediate = builder2.stream(intermediateTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder2.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        intermediate
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        Topology topology2 = builder2.build();
        log.info("Topology2: \n" + topology2.describe());
        KafkaStreams streams2 = new KafkaStreams(topology2, streamsConfiguration2);

        streams1.start();
        streams2.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "300|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "200|fromstream")
                ),
                producerConfig
        );


        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        // The solution works, the join is properly done with a timestamp extractor consideration
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice", "200|fromstream100|fromktable")
        ));

        streams1.close();
        streams2.close();
    }

    @Test
    @Order(5)
    public void givenSameTimestampShouldNotJoinOnGoodValue() throws Exception {
        Properties streamsConfiguration1 = new Properties();
        streamsConfiguration1.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt() + "-1");
        streamsConfiguration1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration1.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration1.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder1 = new StreamsBuilder();
        KStream<String, String> userClicksStream = builder1.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        userClicksStream
                .selectKey((key, value) -> key)
                .to(intermediateTopic, Produced.with(stringSerde, stringSerde));
        Topology topology1 = builder1.build();
        log.info("Topology1: \n" + topology1.describe());
        KafkaStreams streams1 = new KafkaStreams(topology1, streamsConfiguration1);

        Properties streamsConfiguration2 = new Properties();
        streamsConfiguration2.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test" + new Random().nextInt() + "-2");
        streamsConfiguration2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        streamsConfiguration2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration2.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration2.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());

        StreamsBuilder builder2 = new StreamsBuilder();
        KStream<String, String> intermediate = builder2.stream(intermediateTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        KTable<String, String> userRegionsTable = builder2.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()));
        intermediate
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        Topology topology2 = builder2.build();
        log.info("Topology2: \n" + topology2.describe());
        KafkaStreams streams2 = new KafkaStreams(topology2, streamsConfiguration2);

        streams1.start();
        streams2.start();

        Properties producerConfig = producerConfig();
        Properties consumerConfig = consumerConfig();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromktable")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "300|fromktable")
                ),
                producerConfig
        );
        // We publish 2 events on the stream, one with the same exact timestamp as the ktable timestamp we are looking for
        // and one with a timestamp slightly superior
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "100|fromstream")
                ),
                producerConfig
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        new KeyValue<>("alice", "101|fromstream")
                ),
                producerConfig
        );


        List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );

        // The KStream event with timestamp "100" does not matches where "101" matches
        // The join < or <= is conditioned by the order Kafka is polling data, a bug is reported to Confluent
        assertThat(actualClicksPerRegion.size()).isEqualTo(1);
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice", "101|fromstream100|fromktable")
                )
        );

        streams1.close();
        streams2.close();
    }


    private Properties consumerConfig() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "punctuate-test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }

    private Properties producerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerConfig;
    }

    private String join(String left, String right) {
        return left + right;
    }

    /*
    For the demo the timestamp extractor will the value before the pipe in the record value,
    however tomorrow in your JDBC usecase, it can rely on one of the field of the message such as a created_date/updated_date
     */
    private class MyTimestampExtractor implements TimestampExtractor {

        //ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
            // objectMapper.readTree(record.value()).get("created_date");
            return Long.valueOf(record.value().toString().split("\\|")[0]);
        }
    }
}
