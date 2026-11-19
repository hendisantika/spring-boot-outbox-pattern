package id.my.hendisantika.outbox_pattern.outbox;

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
public enum OutboxStatus {
    /**
     * Event sudah tersimpan, menunggu dipublish oleh relay.
     */
    PENDING,
    /**
     * Event sudah di-ack oleh broker Kafka.
     */
    SENT,
    /**
     * Event gagal dipublish setelah melewati batas percobaan, perlu penanganan manual.
     */
    FAILED
}
