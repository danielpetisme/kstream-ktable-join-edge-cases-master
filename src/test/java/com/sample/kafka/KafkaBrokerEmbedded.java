package com.sample.kafka;

import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporter$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static kafka.server.KafkaConfig.*;

public class KafkaBrokerEmbedded {

    private static final Logger log = LoggerFactory.getLogger(KafkaBrokerEmbedded.class);

    private static Map<String, Object> DEFAULT_BROKER_CONFIG = Map.of(
            BrokerIdProp(), 0,
            HostNameProp(), "127.0.0.1",
            PortProp(), 9092,
            NumPartitionsProp(), 1,
            AutoCreateTopicsEnableProp(), true,
            MessageMaxBytesProp(), 1000000,
            ControlledShutdownEnableProp(), true,
            ZkConnectProp(), "127.0.0.1:2181"
    );

    private final File logDir;
    private final KafkaServer kafka;
    public final String listener;

    public KafkaBrokerEmbedded() {
        this(Collections.emptyMap());
    }

    public KafkaBrokerEmbedded(Map<String, Object> config) {
        this(config, null);
    }

    public KafkaBrokerEmbedded(Map<String, Object> config, File logDir) {
        this.logDir = logDir == null ? tmpDir("kafka-broker-embedded-") : logDir;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Utils.delete(logDir);
            } catch (IOException e) {
                log.error("Error deleting {}", logDir.getAbsolutePath(), e);
            }
        }, "delete-temp-file-shutdown-hook"));
        var brokerConfig = new HashMap<>(DEFAULT_BROKER_CONFIG);
        brokerConfig.putAll(config);
        brokerConfig.put(LogDirProp(), this.logDir.getAbsolutePath());
        Option<String> threadPrefixName = Option.apply(KafkaBrokerEmbedded.class.getCanonicalName());
        Seq<KafkaMetricsReporter> reporters = KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(new Properties()));
        kafka = new KafkaServer(new KafkaConfig(brokerConfig), new SystemTime(), threadPrefixName, reporters);
        listener = String.join(":", kafka.config().hostName(), kafka.config().port().toString());
    }

    /**
     * Start the broker.
     */
    public void start() {
        log.debug("Starting embedded Kafka broker at {} (with log.dirs={}) ...", listener, logDir);
        try {
            kafka.startup();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.debug("Startup of embedded Kafka broker at {} completed ...", listener);
    }

    /**
     * Stop the broker.
     */
    public void stop() {
        log.debug("Shutting down embedded Kafka broker at {} ...", listener);
        kafka.shutdown();
        kafka.awaitShutdown();
        log.debug("Removing logs.dir at {} ...", logDir);
        logDir.delete();
        log.debug("Shutdown of embedded Kafka broker at {} completed ...", listener);
    }

    private File tmpDir(String prefix) {
        File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();
        return file;

    }

}
