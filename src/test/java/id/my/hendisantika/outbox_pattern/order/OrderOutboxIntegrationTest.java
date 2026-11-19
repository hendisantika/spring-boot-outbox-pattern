package id.my.hendisantika.outbox_pattern.order;

import id.my.hendisantika.outbox_pattern.TestcontainersConfiguration;
import id.my.hendisantika.outbox_pattern.consumer.ProcessedEventRepository;
import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import id.my.hendisantika.outbox_pattern.outbox.OutboxEvent;
import id.my.hendisantika.outbox_pattern.outbox.OutboxEventRepository;
import id.my.hendisantika.outbox_pattern.outbox.OutboxEventWriter;
import id.my.hendisantika.outbox_pattern.outbox.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.IllegalTransactionStateException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OrderOutboxIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxEventWriter outboxEventWriter;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void createOrder_savesOrderAndOutboxEvent_thenRelayPublishesToKafkaAndConsumerProcessesIt() throws Exception {
        String orderId = createOrder("Hendi", "Keyboard", 2, "150000.50");

        assertThat(orderRepository.findById(Long.valueOf(orderId))).isPresent();

        List<OutboxEvent> events = outboxEvents(orderId);
        assertThat(events).hasSize(1);
        OutboxEvent event = events.getFirst();
        assertThat(event.getEventType()).isEqualTo(OrderEvents.ORDER_CREATED);
        assertThat(event.getTopic()).isEqualTo(OrderEvents.TOPIC);
        JsonNode payload = jsonMapper.readTree(event.getPayload());
        assertThat(payload.get("orderId").asText()).isEqualTo(orderId);
        assertThat(payload.get("totalAmount").decimalValue()).isEqualByComparingTo("300001.00");

        await().atMost(TIMEOUT).untilAsserted(() -> {
            OutboxEvent sent = outboxEventRepository.findById(event.getId()).orElseThrow();
            assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
            assertThat(sent.getSentAt()).isNotNull();
            assertThat(sent.getAttempts()).isEqualTo(1);
        });

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(processedEventRepository.findById(event.getId()))
                        .hasValueSatisfying(processed -> {
                            assertThat(processed.getEventType()).isEqualTo(OrderEvents.ORDER_CREATED);
                            assertThat(processed.getAggregateId()).isEqualTo(orderId);
                        }));
    }

    @Test
    void cancelOrder_writesCancelledEventAfterCreatedEvent() throws Exception {
        String orderId = createOrder("Budi", "Mouse", 1, "75000");

        mockMvc.perform(post("/api/orders/{id}/cancel", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"reason": "Salah pilih produk"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        assertThat(outboxEvents(orderId))
                .extracting(OutboxEvent::getEventType)
                .containsExactly(OrderEvents.ORDER_CREATED, OrderEvents.ORDER_CANCELLED);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<OutboxEvent> events = outboxEvents(orderId);
            assertThat(events).allSatisfy(e -> assertThat(e.getStatus()).isEqualTo(OutboxStatus.SENT));
            assertThat(events).allSatisfy(e ->
                    assertThat(processedEventRepository.existsById(e.getId())).isTrue());
        });
    }

    @Test
    void cancelOrderTwice_returnsConflictAndDoesNotWriteExtraEvent() throws Exception {
        String orderId = createOrder("Citra", "Monitor", 1, "2500000");
        String body = """
                {"reason": "Batal"}
                """;
        mockMvc.perform(post("/api/orders/{id}/cancel", orderId)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/orders/{id}/cancel", orderId)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict());

        assertThat(outboxEvents(orderId)).hasSize(2);
    }

    @Test
    void invalidRequest_doesNotWriteOrderOrOutboxEvent() throws Exception {
        long orders = orderRepository.count();
        long events = outboxEventRepository.count();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerName": "", "product": "Laptop", "quantity": 0, "price": 10}
                                """))
                .andExpect(status().isBadRequest());

        assertThat(orderRepository.count()).isEqualTo(orders);
        assertThat(outboxEventRepository.count()).isEqualTo(events);
    }

    @Test
    void getUnknownOrder_returnsNotFound() throws Exception {
        mockMvc.perform(get("/api/orders/{id}", Long.MAX_VALUE))
                .andExpect(status().isNotFound());
    }

    @Test
    void outboxWriter_requiresExistingTransaction() {
        assertThatThrownBy(() -> outboxEventWriter.write(OrderEvents.TOPIC, OrderEvents.AGGREGATE_TYPE, "1",
                OrderEvents.ORDER_CREATED, "{}"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    private String createOrder(String customer, String product, int quantity, String price) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerName": "%s", "product": "%s", "quantity": %d, "price": %s}
                                """.formatted(customer, product, quantity, price)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andReturn();
        return jsonMapper.readTree(result.getResponse().getContentAsString()).get("id").asText();
    }

    private List<OutboxEvent> outboxEvents(String orderId) {
        return outboxEventRepository.findByAggregateTypeAndAggregateIdOrderByCreatedAt(OrderEvents.AGGREGATE_TYPE,
                orderId);
    }
}
