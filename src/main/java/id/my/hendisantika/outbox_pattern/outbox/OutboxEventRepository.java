package id.my.hendisantika.outbox_pattern.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

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
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Mengambil dan mengunci batch event PENDING tertua. {@code SKIP LOCKED} membuat beberapa instance
     * relay tidak saling menunggu dan tidak mengambil baris yang sama. Harus dipanggil di dalam transaksi.
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at, id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> lockPendingBatch(@Param("limit") int limit);

    List<OutboxEvent> findByAggregateTypeAndAggregateIdOrderByCreatedAt(String aggregateType, String aggregateId);

    long countByStatus(OutboxStatus status);

    @Modifying
    @Query("DELETE FROM OutboxEvent e WHERE e.status = :status AND e.sentAt < :before")
    int deleteByStatusAndSentAtBefore(@Param("status") OutboxStatus status, @Param("before") Instant before);
}
