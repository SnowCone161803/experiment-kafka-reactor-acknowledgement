package gdm.example.kafka.kafka_reactor_acknowledgement;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class FakeKafkaEventHandlerTest {

    FakeKafkaEventHandler fakeKafkaEventHandler = new FakeKafkaEventHandler();

    @Test
    void itShouldShowTheAcknowledgementWhenEmittingSuccessfully() throws Exception {
        fakeKafkaEventHandler.addEventhandler(ev ->
            ev.doOnNext(e -> log.info("event handler 1 happened: {}", e))
        );
        fakeKafkaEventHandler.addEventhandler(ev ->
            ev.doOnNext(e -> log.info("event handler 2 happened: {}", e))
        );
        fakeKafkaEventHandler.startHandlingEvents();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        Thread.sleep(2_000);
    }
    @Test

    void itShouldPutOnTheDeadLetterTopicWhenSomethingFails() throws Exception {
        fakeKafkaEventHandler.addEventhandler(ev ->
            ev.doOnNext(e -> {
                throw new IllegalStateException("event handler failed!");
            }));
        fakeKafkaEventHandler.addEventhandler(ev ->
            ev.doOnNext(e -> log.info("event handler 1 happened: {}", e))
        );
        fakeKafkaEventHandler.addEventhandler(ev ->
            ev.doOnNext(e -> log.info("event handler 2 happened: {}", e))
        );
        fakeKafkaEventHandler.startHandlingEvents();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        fakeKafkaEventHandler.pretendWeReceivedAMessageFromKafka();
        Thread.sleep(2_000);
    }
}
