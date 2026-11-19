package id.my.hendisantika.outbox_pattern.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * Project : outbox-pattern
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Link : s.id/hendisantika
 * Date: 19/11/26
 * Time: 16.30
 */

/**
 * Message relay (polling publisher): membaca event PENDING dari tabel outbox lalu mempublish ke Kafka.
 * Status SENT baru disimpan setelah broker memberi ack, sehingga jaminannya at-least-once: kalau aplikasi
 * mati setelah publish tapi sebelum commit, event akan dipublish ulang. Consumer harus idempotent.
 */
@Component
@ConditionalOnProperty(prefix = "app.outbox.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    public static final String HEADER_EVENT_ID = "eventId";
    public static final String HEADER_EVENT_TYPE = "eventType";
    public static final String HEADER_AGGREGATE_TYPE = "aggregateType";

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxRelay(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                       TransactionTemplate transactionTemplate, OutboxProperties properties, Clock clock) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.outbox.relay.poll-interval:1000}",
            initialDelayString = "${app.outbox.relay.initial-delay:1000}")
    public void relay() {
        int published;
        do {
            published = publishBatch();
        } while (published == properties.batchSize());
    }

    /**
     * Memproses satu batch dalam satu transaksi. Baris yang diambil dikunci ({@code FOR UPDATE SKIP LOCKED})
     * sampai transaksi selesai, jadi instance lain tidak akan mempublish event yang sama secara bersamaan.
     *
     * @return jumlah event yang berhasil dipublish
     */
    public int publishBatch() {
        Integer result = transactionTemplate.execute(status -> {
            List<OutboxEvent> events = repository.lockPendingBatch(properties.batchSize());
            int sent = 0;
            for (OutboxEvent event : events) {
                try {
                    send(event);
                    event.markSent(Instant.now(clock));
                    sent++;
                } catch (Exception ex) {
                    String error = rootMessage(ex);
                    event.markAttemptFailed(error, properties.maxAttempts());
                    log.warn("Gagal publish outbox event {} ({} #{}), percobaan ke-{}: {}", event.getId(),
                            event.getEventType(), event.getAggregateId(), event.getAttempts(), error);
                    if (event.getStatus() == OutboxStatus.FAILED) {
                        log.error("Outbox event {} ditandai FAILED setelah {} percobaan", event.getId(),
                                event.getAttempts());
                    }
                    // Berhenti di event pertama yang gagal supaya urutan event per aggregate tetap terjaga
                    // dan tidak membanjiri broker yang sedang bermasalah.
                    break;
                }
            }
            if (!events.isEmpty()) {
                log.debug("Outbox relay: {} dari {} event berhasil dipublish", sent, events.size());
            }
            return sent;
        });
        return result == null ? 0 : result;
    }

    private void send(OutboxEvent event) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(event.getTopic(), event.getAggregateId(),
                event.getPayload());
        record.headers()
                .add(HEADER_EVENT_ID, event.getId().getBytes(StandardCharsets.UTF_8))
                .add(HEADER_EVENT_TYPE, event.getEventType().getBytes(StandardCharsets.UTF_8))
                .add(HEADER_AGGREGATE_TYPE, event.getAggregateType().getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException ex) {
            throw ex.getCause() instanceof Exception cause ? cause : ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        }
    }

    @Scheduled(cron = "${app.outbox.cleanup.cron:0 0 * * * *}")
    public void cleanup() {
        Instant before = Instant.now(clock).minus(properties.retention());
        Integer deleted = transactionTemplate.execute(status ->
                repository.deleteByStatusAndSentAtBefore(OutboxStatus.SENT, before));
        if (deleted != null && deleted > 0) {
            log.info("Outbox cleanup: {} event SENT sebelum {} dihapus", deleted, before);
        }
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
