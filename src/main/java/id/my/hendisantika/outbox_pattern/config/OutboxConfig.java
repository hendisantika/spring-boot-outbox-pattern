package id.my.hendisantika.outbox_pattern.config;

import id.my.hendisantika.outbox_pattern.order.event.OrderEvents;
import id.my.hendisantika.outbox_pattern.outbox.OutboxProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

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
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxConfig {

    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(OrderEvents.TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
