package company.vk.edu.distrib.compute.andeco.audit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import company.vk.edu.distrib.compute.AuditEvent;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

final class KafkaAuditPublisher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaAuditPublisher.class);
    private static final String TOPIC = "audit";

    private final ReentrantLock lock = new ReentrantLock();
    private String bootstrapServers;
    private boolean async;
    private KafkaProducer<String, String> producer;

    void setBootstrapServers(String bootstrapServers) {
        lock.lock();
        try {
            if (Objects.equals(this.bootstrapServers, bootstrapServers)) {
                return;
            }
            this.bootstrapServers = bootstrapServers;
            closeProducer();
        } finally {
            lock.unlock();
        }
    }

    void setAsync(boolean enabled) {
        this.async = enabled;
    }

    void publish(AuditEvent event) {
        String servers = bootstrapServers;
        if (servers == null || servers.isBlank()) {
            return;
        }
        KafkaProducer<String, String> current = ensureProducer(servers);
        String encoded = AuditEventCoderUtils.encode(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.id(), encoded);

        if (async) {
            current.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.warn("Audit send failed", exception);
                }
            });
        } else {
            try {
                current.send(record).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (log.isWarnEnabled()) {
                    log.warn("Audit send failed", e.getCause());
                }
            }
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closeProducer();
        } finally {
            lock.unlock();
        }
    }

    private KafkaProducer<String, String> ensureProducer(String servers) {
        KafkaProducer<String, String> curr = producer;
        if (curr != null) {
            return curr;
        }
        lock.lock();
        try {
            if (producer == null) {
                producer = new KafkaProducer<>(producerProps(servers));
            }
            return producer;
        } finally {
            lock.unlock();
        }
    }

    private Properties producerProps(String servers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(servers));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private void closeProducer() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
