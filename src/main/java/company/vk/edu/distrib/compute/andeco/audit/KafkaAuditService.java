package company.vk.edu.distrib.compute.andeco.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAuditService implements AuditService {
    private static final Logger log = LoggerFactory.getLogger(KafkaAuditService.class);
    private static final String TOPIC = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AuditFileStore store;
    private final Properties consumerProps;

    private Thread workerThread;
    private KafkaConsumer<String, String> consumer;

    public KafkaAuditService(String bootstrapServers, String consumerGroupId) throws IOException {
        Path file = Path.of("../audit-" + consumerGroupId + "-" + UUID.randomUUID() + ".log");
        this.store = new AuditFileStore(file);

        this.consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        KafkaConsumer<String, String> currentConsumer = new KafkaConsumer<>(consumerProps);
        consumer = currentConsumer;

        Thread thread = new Thread(() -> pollLoop(currentConsumer),
                "AuditConsumer-" + consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        thread.setDaemon(true);
        workerThread = thread;
        thread.start();
    }

    @Override
    public void stop() {
        running.set(false);

        KafkaConsumer<String, String> currentConsumer = consumer;
        if (currentConsumer != null) {
            currentConsumer.wakeup();
        }

        Thread currentThread = workerThread;
        if (currentThread != null) {
            try {
                currentThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return store.snapshot();
    }

    private void pollLoop(KafkaConsumer<String, String> currentConsumer) {
        try (currentConsumer) {
            currentConsumer.subscribe(Collections.singletonList(TOPIC));

            while (running.get()) {
                var records = currentConsumer.poll(POLL_TIMEOUT);
                for (var record : records) {
                    String value = record.value();
                    if (value == null || value.isBlank()) {
                        log.warn("Skip empty audit record");
                        continue;
                    }

                    try {
                        AuditEvent event = AuditEventCoderUtils.decode(value);
                        store.append(event);
                    } catch (IllegalArgumentException e) {
                        log.warn("Skip invalid audit record: '{}'", value, e);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            // shutdown
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            running.set(false);
        }
    }
}
