package gdm.example.kafka.kafka_reactor_acknowledgement;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class FakeKafkaEventHandlerTest {

    FakeKafkaEventHandler fakeKafkaEventHandler = new FakeKafkaEventHandler();

    @Test
    void itShouldShowTheAcknowledgementWhenEmitting() throws Exception {
        fakeKafkaEventHandler.acceptEventHandler(ev ->
            ev.doOnNext(e -> log.info("event happened: {}", e))
        );
        fakeKafkaEventHandler.startHandlingEvents();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        Thread.sleep(2_000);
    }
}
