package gdm.example.kafka.kafka_reactor_acknowledgement;


import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.util.Tuple;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Service
@Slf4j
public class FakeKafkaEventHandler {

    private final Sinks.Many<Tuple<Mono<MyEvent>, FakeAcknowledgement>> eventSink = Sinks.many().multicast().onBackpressureBuffer();
//    private final Sinks.Many<Function<Mono<MyEvent>, Publisher<Object>>> eventHandlers = Sinks.many().replay().all();
    private final List<Function<Mono<MyEvent>, Publisher<Object>>> eventHandlers = new LinkedList<>();
    private Flux<Tuple<Mono<MyEvent>, FakeAcknowledgement>> events;
    private final AtomicInteger count = new AtomicInteger();

    public <T> void addEventhandler(Function<Mono<MyEvent>, Publisher<T>> eventHandler) {
        eventHandlers.add(convertEventHandler(eventHandler));
    }

    private <T> Function<Mono<MyEvent>, Publisher<Object>> convertEventHandler(
        Function<Mono<MyEvent>, Publisher<T>> eventHandler
    ) {
        return (Mono<MyEvent> ev) -> ev.transform(eventHandler).map(e -> new Object());
    }

    public Flux<Object> applyAllEventHandlers(Mono<MyEvent> event, FakeAcknowledgement acknowledgement) {
        final var handledEvents = Flux.fromIterable(this.eventHandlers)
            .map(event::transform);
        return Flux.merge(handledEvents)
            .doOnComplete(acknowledgement::acknowledge)
            .onErrorResume(err -> {
                log.error("event failed", err);
                acknowledgement.rejectToDeadLetterTopic();
                return Mono.empty();
            });
    }

    public void startHandlingEvents() {
        eventSink
            .asFlux()
            .flatMap(t -> applyAllEventHandlers(t._1(), t._2()))
            .subscribe();

    }

    public void pretendWeReceivedAMessageFromKafka() {
        final var name = "event-" + count.getAndIncrement();
        final var event = new MyEvent(name);
        final var acknowlegement = new FakeAcknowledgement(event);
        this.handleEvent(event, acknowlegement);
    }

    public void handleEvent(MyEvent event, FakeAcknowledgement acknowledgement) {
        eventSink.emitNext(
            new Tuple<>(Mono.just(event), acknowledgement),
            Sinks.EmitFailureHandler.FAIL_FAST);
    }
}
