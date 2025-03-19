package gdm.example.kafka.kafka_reactor_acknowledgement;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class FakeAcknowledgement {

    private final MyEvent event;


    public void acknowledge() {
        log.info("event [{}] acknowledged", event.name());
    }

    public void rejectToDeadLetterTopic() {
        log.error("event [{}] put on DLT", event.name());
    }
}
