package gdm.example.kafka.kafka_reactor_acknowledgement;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

import java.util.HashMap;

@Slf4j
public class AcknowedgementSubscriber implements CoreSubscriber<Object> {

    private Context context = Context.of(
        FakeAcknowledgement.class, new HashMap<Object, FakeAcknowledgement>(),
        "other", "value");

    @Override
    public Context currentContext() {
        return context;
    }


    @Override
    public void onSubscribe(Subscription s) {
        log.info("onSubscribe ctx: {}", this.currentContext());
        s.request(Long.MAX_VALUE);

    }

    @Override
    public void onNext(Object myEvent) {
        log.info("onNext {}, ctx: {}", myEvent, this.currentContext());
    }

    @Override
    public void onError(Throwable t) {

        log.info("onError: {} ctx: {}",
            t,
            this.currentContext());
    }

    @Override
    public void onComplete() {
        log.info("onComplete ctx: {}", this.currentContext());
    }
}
