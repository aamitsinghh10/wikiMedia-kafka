package springboot_kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class WikiMediaChangesHandler implements EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangesHandler.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public WikiMediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        LOGGER.info("Opened stream to Wikimedia recent changes.");
    }

    @Override
    public void onClosed() {
        LOGGER.info("Closed stream to Wikimedia recent changes.");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        LOGGER.info("Received event: {}", messageEvent.getData());
        kafkaTemplate.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String comment) {
        LOGGER.info("Received comment: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error in Wikimedia stream: {}", t.getMessage(), t);
    }
}
