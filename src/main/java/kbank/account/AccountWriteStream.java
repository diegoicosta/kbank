package kbank.account;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kbank.gateway.Command;
import kbank.kafka.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kbank.ApplicationProps.*;
import static kbank.account.CreditDebitCommand.fromJson;

public class AccountWriteStream {

    private Logger log = LoggerFactory.getLogger(this.getClass());
    private ObjectMapper mapper = new ObjectMapper();

    private ReadOnlyKeyValueStore<String, Long> balanceByAccountStore = new ReadOnlyKeyValueStore<String, Long>() {
        @Override
        public Long get(final String s) {
            return null;
        }

        @Override
        public KeyValueIterator<String, Long> range(final String s, final String k1) {
            return null;
        }

        @Override
        public KeyValueIterator<String, Long> all() {
            return null;
        }

        @Override
        public long approximateNumEntries() {
            return 0;
        }
    };
    private AccountInvariances invariances;
    private ActorRef router;

    public AccountWriteStream(ActorRef router) {
        this.router = router;
        startStreams();
    }

    private void startStreams() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, JsonNode> accountStreams[] = builder.stream(
                accountTopic,
                Consumed.with(Serdes.String(), new JsonSerde()))
                .branch(
                        (account, jsonCommand) -> invariances.hasBalance(account, fromJson(jsonCommand).getValue()),
                        (account, jsonCommand) -> invariances.noBalance(account, fromJson(jsonCommand).getValue())
                );

        KGroupedStream<String, Long> groupedAccount = groupValueByAccount(accountStreams[0]);
        materializeAggregation(groupedAccount);

        accountStreams[1]
                .through(accountRefusedTopic)
                .process(() -> ResponseNotifierProcessor.changeRejectedProcessor(router));

        KafkaStreams streams = new KafkaStreams(builder.build(), getAccountStreamProps());

        streams.setStateListener((newState, oldState) -> {
                    // Check when state-store is totally loaded
                    if (newState.equals(KafkaStreams.State.RUNNING) &&
                            oldState.equals(KafkaStreams.State.REBALANCING)) {
                        balanceByAccountStore = streams.store(accountStore, QueryableStoreTypes.keyValueStore());
                        new ResponseStream(router, balanceByAccountStore);
                        invariances = new AccountInvariances(balanceByAccountStore);
                    }
                }
        );

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private KTable<String, Long> materializeAggregation(final KGroupedStream<String, Long> groupedAccount) {
        return groupedAccount.aggregate(
                () -> 0L,
                (accountId, balance, oldBalance) -> oldBalance + balance,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>
                        as(accountStore)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
        );
    }

    private KGroupedStream<String, Long> groupValueByAccount(final KStream<String, JsonNode> accountStream) {
        return accountStream.through(accountAcceptedTopic)
                .mapValues((jsonNode) -> {
                    Command command = mapper.convertValue(jsonNode, Command.class);
                    long value = command.getCommand().get("value").asLong();
                    return command.getType().equals(Command.CommandType.DEBIT) ? value * -1 : value;
                }).groupByKey();
    }

}
