package kbank.gateway;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kbank.ApplicationProps;
import kbank.gateway.Command.CommandType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kbank.gateway.Command.CommandType.*;

public class CommandStream {

    private ActorRef router;
    private ObjectMapper mapper = new ObjectMapper();
    private Logger log = LoggerFactory.getLogger(this.getClass());

    public CommandStream(ActorRef router) {
        this.router = router;
        startStream();
    }


    @SuppressWarnings("unchecked")
    private void startStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode>[] streamBranches = builder.stream(
                ApplicationProps.commandTopic,
                Consumed.with(Serdes.String(),
                        Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer()))

        ).map((KeyValueMapper<String, JsonNode, KeyValue<String, JsonNode>>) (key, value) ->
                mapByCommandType(key, value)
        ).branch(buildPredicate(CREDIT),
                buildPredicate(DEBIT),
                buildPredicate(READ),
                buildPredicate(TRANSFER)
        );
        streamBranches[0].to(ApplicationProps.accountTopic);
        streamBranches[1].to(ApplicationProps.accountTopic);
        streamBranches[2].to(ApplicationProps.accountReadTopic);
        streamBranches[3].to(ApplicationProps.transferTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), ApplicationProps.getCommandStreamProps());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private KeyValue<String, JsonNode> mapByCommandType(final String key, final JsonNode value) {
        CommandType type = CommandType.valueOf(value.get("type").asText());
        if (type.equals(CREDIT) || type.equals(DEBIT) || type.equals(READ)) {
            String account = value.get("command").get("account").asText();
            return new KeyValue<>(account, value);
        }
        return new KeyValue<>(key, value);
    }

    private Predicate<String, JsonNode> buildPredicate(CommandType type) {
        return (id, jsonNode) -> {
            try {
                return mapper.treeToValue(jsonNode, Command.class).getType().equals(type);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
