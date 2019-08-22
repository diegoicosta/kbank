package kbank;

import kbank.kafka.serde.JsonSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.ResourceBundle;

public class ApplicationProps {
    private static ResourceBundle appBundle = ResourceBundle.getBundle("application");

    public static final String brokerUrl = appBundle.getString("kafka.broker.url");
    public static final String accountTopic = appBundle.getString("kafka.account.topic");
    public static final String accountAcceptedTopic = appBundle.getString("kafka.account.accepted.topic");
    public static final String accountRefusedTopic = appBundle.getString("kafka.account.refused.topic");
    public static final String transferTopic = appBundle.getString("kafka.transfer.topic");
    public static final String accountReadTopic = appBundle.getString("kafka.account.read.topic");
    public static final String commandTopic = appBundle.getString("kafka.command.topic");
    public static final String accountStore = appBundle.getString("kafka.account.store");

    public static Properties getKafkaProducerProps() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kbank-prod");

        // to guarantee the strongest consistency: EXACTLY ONCE semantic
        props.put(ProducerConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.producer.client.id"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private static Properties getKafkaStreamProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        // Only because is running locally with one instance
        props.put("transaction.state.log.replication.factor", "1");
        props.put("transaction.state.log.min.isr", "1");

        return props;
    }

    public static Properties getCommandStreamProps() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appBundle.getString("kafka.stream.command.application.id"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.stream.command.client.id"));

        return props;
    }

    public static Properties getAccountAcceptedStreamProps() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appBundle.getString("kafka.stream.account.accepted.application.id"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.stream.account.accepted.client.id"));

        return props;
    }

    public static Properties getAccountRefusedStreamProps() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appBundle.getString("kafka.stream.account.refused.application.id"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.stream.account.refused.client.id"));

        return props;
    }

    public static Properties getAccountStreamProps() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appBundle.getString("kafka.stream.account.application.id"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.stream.account.client.id"));

        return props;
    }

    public static Properties getAccountReadStreamProps() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appBundle.getString("kafka.stream.account-read.application.id"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appBundle.getString("kafka.stream.account-read.client.id"));

        return props;
    }
    
}
