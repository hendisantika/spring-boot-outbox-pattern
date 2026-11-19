package id.my.hendisantika.outbox_pattern.order.dto;

import id.my.hendisantika.outbox_pattern.order.Order;
import id.my.hendisantika.outbox_pattern.order.OrderStatus;

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
public record OrderResponse(
        Long id,
        String customerName,
        String product,
        int quantity,
        BigDecimal price,
        BigDecimal totalAmount,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getCustomerName(), order.getProduct(), order.getQuantity(),
                order.getPrice(), order.getTotalAmount(), order.getStatus(), order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
