package kbank.response;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.HashMap;
import java.util.Map;

public class ResponseActorRouter extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final Map<String, ActorRef> responseReceivers = new HashMap<>();

    public static Props props() {
        return Props.create(ResponseActorRouter.class, () -> new ResponseActorRouter());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ResponseWanted.class, this::registerActor)
                .match(CommandResponse.class, this::notifyResponse)
                .build();
    }

    private void notifyResponse(CommandResponse commandResponse) {
        ActorRef actorRef = responseReceivers.remove(commandResponse.getId());
        if (actorRef != null) {
            log.info("Command {} response ready to be routed to sender", commandResponse.getId());
            actorRef.tell(commandResponse, self());
        } else {
            log.warning("No actor receiver found for command {}", commandResponse.getId());
        }
    }

    private void registerActor(ResponseWanted responseWanted) {
        log.info("Registering actor for {} request response ", responseWanted.getCommandId());
        responseReceivers.put(responseWanted.getCommandId(), getSender());
    }

}
