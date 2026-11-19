package id.my.hendisantika.outbox_pattern.outbox;

import id.my.hendisantika.outbox_pattern.TestcontainersConfiguration;
import id.my.hendisantika.outbox_pattern.order.Order;
import id.my.hendisantika.outbox_pattern.order.OrderService;
import id.my.hendisantika.outbox_pattern.order.dto.CreateOrderRequest;
import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Created by IntelliJ IDEA.
 * Project : outbox-pattern
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Link : s.id/hendisantika
 * Date: 19/11/26
 * Time: 17.19
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OutboxRelayShedLockIntegrationTest {

    private static final String RELAY_LOCK = "outbox-relay";

    @Autowired
    private LockProvider lockProvider;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void relay_acquiresShedLockRow() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM shedlock WHERE name = ?",
                        Integer.class, RELAY_LOCK)).isEqualTo(1));
    }

    @Test
    void relay_doesNotPublish_whileAnotherInstanceHoldsTheLock() {
        // Simulasi instance lain sedang memegang lock relay
        SimpleLock lock = acquireRelayLock();
        String eventId;
        try {
            Order order = orderService.create(new CreateOrderRequest("Dewi", "Headset", 1, new BigDecimal("450000")));
            eventId = outboxEventRepository.findByAggregateTypeAndAggregateIdOrderByCreatedAt(
                    OrderEvents.AGGREGATE_TYPE, String.valueOf(order.getId())).getFirst().getId();

            // Relay di instance ini tetap dijadwalkan setiap 200 ms, tapi harus skip karena lock dipegang pihak lain
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(status(eventId)).isEqualTo(OutboxStatus.PENDING));
        } finally {
            lock.unlock();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(status(eventId)).isEqualTo(OutboxStatus.SENT));
    }

    private SimpleLock acquireRelayLock() {
        // Relay di context ini mungkin sedang memegang lock, jadi coba sampai berhasil
        AtomicReference<SimpleLock> holder = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() -> {
            Optional<SimpleLock> lock = lockProvider.lock(new LockConfiguration(Instant.now(), RELAY_LOCK,
                    Duration.ofSeconds(30), Duration.ZERO));
            lock.ifPresent(holder::set);
            return lock.isPresent();
        });
        return holder.get();
    }

    private OutboxStatus status(String eventId) {
        return outboxEventRepository.findById(eventId).orElseThrow().getStatus();
    }
}
