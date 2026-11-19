package id.my.hendisantika.outbox_pattern.order;

import id.my.hendisantika.outbox_pattern.order.dto.CreateOrderRequest;
import id.my.hendisantika.outbox_pattern.order.event.OrderCancelledEvent;
import id.my.hendisantika.outbox_pattern.order.event.OrderCreatedEvent;
import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import id.my.hendisantika.outbox_pattern.outbox.OutboxEventWriter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventWriter outboxEventWriter;
    private final Clock clock;

    public OrderService(OrderRepository orderRepository, OutboxEventWriter outboxEventWriter, Clock clock) {
        this.orderRepository = orderRepository;
        this.outboxEventWriter = outboxEventWriter;
        this.clock = clock;
    }

    /**
     * Menyimpan order dan event {@code OrderCreated} di transaksi yang sama. Tidak ada panggilan ke Kafka di sini:
     * kalau transaksi rollback, event juga ikut hilang; kalau commit, relay pasti akan mempublish event-nya.
     */
    @Transactional
    public Order create(CreateOrderRequest request) {
        Order order = orderRepository.save(new Order(request.customerName(), request.product(), request.quantity(),
                request.price(), Instant.now(clock)));

        outboxEventWriter.write(OrderEvents.TOPIC, OrderEvents.AGGREGATE_TYPE, String.valueOf(order.getId()),
                OrderEvents.ORDER_CREATED, OrderCreatedEvent.from(order));
        return order;
    }

    @Transactional
    public Order cancel(Long id, String reason) {
        Order order = getById(id);
        order.cancel(Instant.now(clock));

        outboxEventWriter.write(OrderEvents.TOPIC, OrderEvents.AGGREGATE_TYPE, String.valueOf(order.getId()),
                OrderEvents.ORDER_CANCELLED, OrderCancelledEvent.from(order, reason));
        return order;
    }

    @Transactional(readOnly = true)
    public Order getById(Long id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}
