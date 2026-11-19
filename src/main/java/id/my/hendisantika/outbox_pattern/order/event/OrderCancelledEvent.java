package id.my.hendisantika.outbox_pattern.order.event;

import id.my.hendisantika.outbox_pattern.order.Order;

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
public record OrderCancelledEvent(Long orderId, String reason, Instant occurredAt) {

    public static OrderCancelledEvent from(Order order, String reason) {
        return new OrderCancelledEvent(order.getId(), reason, order.getUpdatedAt());
    }
}
