package kbank.gateway;

import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.StringJoiner;
import java.util.UUID;

public class Command {
    protected String id;
    protected CommandType type;
    protected JsonNode command;
    protected ActorRef router;


    public void setId(final String id) {
        this.id = id;
    }

    public void setCommand(final JsonNode command) {
        this.command = command;
    }

    public String getId() {
        return id;
    }

    public JsonNode getCommand() {
        return command;
    }

    public ActorRef getRouter() {
        return router;
    }

    public void setRouter(final ActorRef router) {
        this.router = router;
    }

    public CommandType getType() {
        return type;
    }

    public void setType(final CommandType type) {
        this.type = type;
    }

    public enum CommandType {
        TRANSFER, READ, CREDIT, DEBIT
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Command.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("type=" + type)
                .add("command=" + command)
                .add("router=" + router)
                .toString();
    }

    public static Command asReadCommand(final String accountId) {
        Command readCmd = new Command();
        readCmd.setId(UUID.randomUUID().toString());
        readCmd.setType(Command.CommandType.READ);
        ObjectNode cmdNode = JsonNodeFactory.instance.objectNode();
        cmdNode.put("account", accountId);
        readCmd.setCommand(cmdNode);
        return readCmd;
    }
}
