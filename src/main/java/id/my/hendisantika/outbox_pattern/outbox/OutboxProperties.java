package id.my.hendisantika.outbox_pattern.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

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
 * Konfigurasi outbox relay.
 *
 * @param batchSize     jumlah maksimum event yang diambil per polling
 * @param maxAttempts   batas percobaan publish sebelum event ditandai FAILED
 * @param sendTimeout   waktu tunggu ack dari broker per event
 * @param retention     lama event SENT disimpan sebelum dihapus oleh cleanup
 */
@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        @DefaultValue("100") int batchSize,
        @DefaultValue("10") int maxAttempts,
        @DefaultValue("10s") Duration sendTimeout,
        @DefaultValue("7d") Duration retention) {
}
