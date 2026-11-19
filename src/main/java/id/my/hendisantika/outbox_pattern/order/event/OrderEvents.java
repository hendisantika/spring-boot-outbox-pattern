package id.my.hendisantika.outbox_pattern.order.event;

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
public final class OrderEvents {

    public static final String TOPIC = "order.events";
    public static final String AGGREGATE_TYPE = "Order";
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String ORDER_CANCELLED = "OrderCancelled";

    private OrderEvents() {
    }
}
