package id.my.hendisantika.outbox_pattern.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
class OutboxRelayTest {

    private static final Instant NOW = Instant.parse("2026-11-19T09:30:00Z");

    private OutboxEventRepository repository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private OutboxRelay relay;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);
        when(transactionTemplate.execute(any())).thenAnswer(invocation ->
                invocation.<TransactionCallback<?>>getArgument(0).doInTransaction(new SimpleTransactionStatus()));

        OutboxProperties properties = new OutboxProperties(100, 3, Duration.ofSeconds(1), Duration.ofDays(7));
        relay = new OutboxRelay(repository, kafkaTemplate, transactionTemplate, properties,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void publishBatch_sendsEventWithKeyAndHeaders_thenMarksSent() {
        OutboxEvent event = event("10");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(success());

        int sent = relay.publishBatch();

        assertThat(sent).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(event.getSentAt()).isEqualTo(NOW);
        assertThat(event.getAttempts()).isEqualTo(1);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("order.events");
        assertThat(record.key()).isEqualTo("10");
        assertThat(record.value()).isEqualTo("{\"orderId\":10}");
        assertThat(new String(record.headers().lastHeader(OutboxRelay.HEADER_EVENT_ID).value(),
                StandardCharsets.UTF_8)).isEqualTo(event.getId());
        assertThat(new String(record.headers().lastHeader(OutboxRelay.HEADER_EVENT_TYPE).value(),
                StandardCharsets.UTF_8)).isEqualTo("OrderCreated");
    }

    @Test
    void publishBatch_stopsAtFirstFailure_toPreserveOrdering() {
        OutboxEvent first = event("1");
        OutboxEvent second = event("1");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        int sent = relay.publishBatch();

        assertThat(sent).isZero();
        assertThat(first.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(first.getAttempts()).isEqualTo(1);
        assertThat(first.getLastError()).contains("broker down");
        assertThat(second.getAttempts()).isZero();
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void publishBatch_marksEventFailed_afterMaxAttempts() {
        OutboxEvent event = event("7");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("record too large")));

        relay.publishBatch();
        relay.publishBatch();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);

        relay.publishBatch();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(event.getAttempts()).isEqualTo(3);
    }

    private static OutboxEvent event(String aggregateId) {
        return OutboxEvent.pending("Order", aggregateId, "OrderCreated", "order.events",
                "{\"orderId\":" + aggregateId + "}", NOW);
    }

    private static CompletableFuture<SendResult<String, String>> success() {
        return CompletableFuture.completedFuture(null);
    }
}
