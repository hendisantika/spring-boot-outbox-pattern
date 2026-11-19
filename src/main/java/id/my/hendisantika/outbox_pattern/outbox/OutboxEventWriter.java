package id.my.hendisantika.outbox_pattern.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

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
@Component
public class OutboxEventWriter {

    private final OutboxEventRepository repository;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public OutboxEventWriter(OutboxEventRepository repository, JsonMapper jsonMapper, Clock clock) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /**
     * Menyimpan event ke tabel outbox di dalam transaksi pemanggil. {@link Propagation#MANDATORY} memastikan
     * event tidak pernah ditulis di luar transaksi bisnis, sehingga perubahan data dan event selalu atomik.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent write(String topic, String aggregateType, String aggregateId, String eventType, Object payload) {
        String json = jsonMapper.writeValueAsString(payload);
        OutboxEvent event = OutboxEvent.pending(aggregateType, aggregateId, eventType, topic, json, Instant.now(clock));
        return repository.save(event);
    }
}
