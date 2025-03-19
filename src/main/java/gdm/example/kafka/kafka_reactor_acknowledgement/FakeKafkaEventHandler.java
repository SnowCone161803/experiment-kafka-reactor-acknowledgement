package gdm.example.kafka.kafka_reactor_acknowledgement;


import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.util.Tuple;
import reactor.core.Disposable;
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
    private final List<Function<Mono<MyEvent>, Publisher<Object>>> eventHandlers = new LinkedList<>();
    private Flux<Tuple<Mono<MyEvent>, FakeAcknowledgement>> events;
    private final AtomicInteger count = new AtomicInteger();
    private Disposable disposable;

    public <T> void addEventHandler(Function<Mono<MyEvent>, Publisher<T>> eventHandler) {
        eventHandlers.add(ignoreEventHandlerReturnValue(eventHandler));
    }


    @SuppressWarnings("unchecked")
    private <T> Function<Mono<MyEvent>, Publisher<Object>> ignoreEventHandlerReturnValue(
        Function<Mono<MyEvent>, Publisher<T>> eventHandler
    ) {
        return (Mono<MyEvent> ev) -> (Publisher<Object>) ev.transform(eventHandler);
    }

    public Flux<Object> applyEventHandlersAndAcknowledge(Mono<MyEvent> event, FakeAcknowledgement acknowledgement) {
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

    @Synchronized
    public void start() {
        if (disposable != null) {
            log.warn("already started");
            return;
        }
        disposable = eventSink
            .asFlux()
            .flatMap(t -> applyEventHandlersAndAcknowledge(t._1(), t._2()))
            .subscribe();
    }

    @Synchronized
    public void stop() {
        disposable.dispose();
        disposable = null;
    }

    public void pretendWeReceivedAMessageFromKafka() {
        final var name = "event-" + count.getAndIncrement();
        final var event = new MyEvent(name);
        final var acknowledgement = new FakeAcknowledgement(event);
        this.methodCalledByKafkaJavaClient(event, acknowledgement);
    }

    public void methodCalledByKafkaJavaClient(MyEvent event, FakeAcknowledgement acknowledgement) {
        eventSink.emitNext(
            new Tuple<>(Mono.just(event), acknowledgement),
            Sinks.EmitFailureHandler.FAIL_FAST);
    }
}
