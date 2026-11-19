package id.my.hendisantika.outbox_pattern.consumer;

import id.my.hendisantika.outbox_pattern.TestcontainersConfiguration;
import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import id.my.hendisantika.outbox_pattern.outbox.OutboxRelay;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OrderEventListenerIdempotencyTest {

    @Autowired
    private OrderEventListener listener;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void duplicateDelivery_isProcessedOnlyOnce() {
        String eventId = UUID.randomUUID().toString();
        long before = processedEventRepository.count();

        // Simulasi redelivery: pesan dengan eventId yang sama diterima dua kali
        listener.onOrderEvent(record(eventId, 0));
        listener.onOrderEvent(record(eventId, 1));

        assertThat(processedEventRepository.count()).isEqualTo(before + 1);
        assertThat(processedEventRepository.findById(eventId)).isPresent();
    }

    @Test
    void messageWithoutEventIdHeader_isIgnored() {
        long before = processedEventRepository.count();

        listener.onOrderEvent(new ConsumerRecord<>(OrderEvents.TOPIC, 0, 0, "99", "{}"));

        assertThat(processedEventRepository.count()).isEqualTo(before);
    }

    private static ConsumerRecord<String, String> record(String eventId, long offset) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(OrderEvents.TOPIC, 0, offset, "42",
                "{\"orderId\":42}");
        record.headers()
                .add(OutboxRelay.HEADER_EVENT_ID, eventId.getBytes(StandardCharsets.UTF_8))
                .add(OutboxRelay.HEADER_EVENT_TYPE, OrderEvents.ORDER_CREATED.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
