package kbank.gateway;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import kbank.response.ResponseWanted;

public class CommandActorHandler extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private String id;
    private CommandKafkaProducer producer;

    public CommandActorHandler(final String id, final CommandKafkaProducer producer) {
        this.id = id;
        this.producer = producer;
    }

    public static Props props(String id, CommandKafkaProducer producer) {
        return Props.create(CommandActorHandler.class, () -> new CommandActorHandler(id, producer));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Command.class, this::handleCommand)
                .build();
    }

    private void handleCommand(Command command) {
        log.info("Sending command  to kafka {}", command);
        command.getRouter().tell(new ResponseWanted(command.getId()), getSender());
        producer.send(command);
    }
    
}
