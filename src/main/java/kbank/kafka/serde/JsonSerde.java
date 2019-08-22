package kbank.kafka.serde;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

public class JsonSerde implements Serde<JsonNode> {

    @Override
    public Serializer<JsonNode> serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return new JsonDeserializer();
    }
}
