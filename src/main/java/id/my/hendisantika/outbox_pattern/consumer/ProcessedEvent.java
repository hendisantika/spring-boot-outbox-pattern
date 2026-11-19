package id.my.hendisantika.outbox_pattern.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", length = 36, nullable = false, updatable = false)
    private String eventId;

    @Column(name = "event_type", nullable = false, updatable = false, length = 100)
    private String eventType;

    @Column(name = "aggregate_id", nullable = false, updatable = false, length = 100)
    private String aggregateId;

    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
        // untuk JPA
    }

    public ProcessedEvent(String eventId, String eventType, String aggregateId, Instant processedAt) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.processedAt = processedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
