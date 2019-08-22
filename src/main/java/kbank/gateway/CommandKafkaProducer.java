package kbank.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kbank.ApplicationProps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandKafkaProducer {

    private KafkaProducer<String, JsonNode> kafkaProducer;
    private Logger log = LoggerFactory.getLogger(this.getClass());

    public CommandKafkaProducer() {
        kafkaProducer = new KafkaProducer<>(ApplicationProps.getKafkaProducerProps());
        kafkaProducer.initTransactions();
    }

    public void send(final Command command) {
        log.info("Persisting command {} to topic {}", command.getId(), ApplicationProps.commandTopic);

        ObjectNode jsonCmd = JsonNodeFactory.instance.objectNode();
        jsonCmd.set("command", command.command);
        jsonCmd.put("id", command.id);
        jsonCmd.put("type", command.type.toString());

        try {
            kafkaProducer.beginTransaction();
            ProducerRecord record = new ProducerRecord<>(
                    ApplicationProps.commandTopic,
                    command.getId(),
                    (JsonNode) jsonCmd
            );

            kafkaProducer.send(record);
            kafkaProducer.commitTransaction();
        } catch (KafkaException e) {
            log.error("Kafka Producer error", e);
            kafkaProducer.abortTransaction();
        }
    }

}
