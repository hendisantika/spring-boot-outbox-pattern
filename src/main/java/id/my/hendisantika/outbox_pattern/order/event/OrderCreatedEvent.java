package id.my.hendisantika.outbox_pattern.order.event;

import id.my.hendisantika.outbox_pattern.order.Order;

import java.math.BigDecimal;
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
public record OrderCreatedEvent(
        Long orderId,
        String customerName,
        String product,
        int quantity,
        BigDecimal price,
        BigDecimal totalAmount,
        Instant occurredAt) {

    public static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(order.getId(), order.getCustomerName(), order.getProduct(), order.getQuantity(),
                order.getPrice(), order.getTotalAmount(), order.getCreatedAt());
    }
}
