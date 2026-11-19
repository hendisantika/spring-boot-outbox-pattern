package id.my.hendisantika.outbox_pattern.consumer;

import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import id.my.hendisantika.outbox_pattern.outbox.OutboxRelay;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;

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
 * Contoh consumer downstream. Relay menjamin at-least-once, jadi event yang sama bisa diterima lebih dari sekali.
 * Consumer mencatat {@code eventId} di tabel {@code processed_events} dalam transaksi yang sama dengan efek bisnisnya,
 * sehingga duplikat cukup di-skip (idempotent consumer / inbox).
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final ProcessedEventRepository processedEventRepository;
    private final Clock clock;

    public OrderEventListener(ProcessedEventRepository processedEventRepository, Clock clock) {
        this.processedEventRepository = processedEventRepository;
        this.clock = clock;
    }

    @KafkaListener(topics = OrderEvents.TOPIC, groupId = "${app.consumer.order-events.group-id:order-notification}")
    @Transactional
    public void onOrderEvent(ConsumerRecord<String, String> record) {
        String eventId = header(record, OutboxRelay.HEADER_EVENT_ID);
        String eventType = header(record, OutboxRelay.HEADER_EVENT_TYPE);
        if (eventId == null || eventType == null) {
            log.warn("Pesan tanpa header eventId/eventType di {}-{}@{} diabaikan", record.topic(),
                    record.partition(), record.offset());
            return;
        }

        if (processedEventRepository.existsById(eventId)) {
            log.info("Event {} ({}) sudah pernah diproses, di-skip", eventId, eventType);
            return;
        }

        switch (eventType) {
            case OrderEvents.ORDER_CREATED ->
                    log.info("Kirim notifikasi order baru #{}: {}", record.key(), record.value());
            case OrderEvents.ORDER_CANCELLED ->
                    log.info("Kirim notifikasi pembatalan order #{}: {}", record.key(), record.value());
            default -> log.debug("Event type {} tidak ditangani", eventType);
        }

        processedEventRepository.save(new ProcessedEvent(eventId, eventType, record.key(), Instant.now(clock)));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
