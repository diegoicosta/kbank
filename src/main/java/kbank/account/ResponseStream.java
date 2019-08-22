package kbank.account;

import akka.actor.ActorRef;
import kbank.kafka.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kbank.ApplicationProps.*;

public class ResponseStream {

    private final ReadOnlyKeyValueStore<String, Long> balanceByAccountStore;
    private final ActorRef router;
    private Logger log = LoggerFactory.getLogger(this.getClass());

    public ResponseStream(ActorRef router, ReadOnlyKeyValueStore<String, Long> balanceByAccountStore) {
        this.router = router;
        this.balanceByAccountStore = balanceByAccountStore;
        startReadStream();
        startAccountAcceptedStream();
        startAccountRefusedStream();
    }

    private void startReadStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(
                accountReadTopic,
                Consumed.with(Serdes.String(), new JsonSerde())
        ).process(() -> ResponseNotifierProcessor.readProcessor(router, balanceByAccountStore));

        KafkaStreams streams = new KafkaStreams(builder.build(), getAccountReadStreamProps());

        log.info("Starting stream of topic {}", accountReadTopic);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private void startAccountAcceptedStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(
                accountAcceptedTopic,
                Consumed.with(Serdes.String(), new JsonSerde())
        ).process(() -> ResponseNotifierProcessor.changeAcceptedProcessor(router));

        KafkaStreams streams = new KafkaStreams(builder.build(), getAccountAcceptedStreamProps());

        log.info("Starting stream of topic {}", accountAcceptedTopic);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private void startAccountRefusedStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(
                accountRefusedTopic,
                Consumed.with(Serdes.String(), new JsonSerde())
        ).process(() -> ResponseNotifierProcessor.changeRejectedProcessor(router));

        KafkaStreams streams = new KafkaStreams(builder.build(), getAccountRefusedStreamProps());

        log.info("Starting stream of topic {}", accountRefusedTopic);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
