package kbank.account;

import akka.actor.ActorRef;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kbank.gateway.Command;

import java.util.StringJoiner;

public class CreditDebitCommand {
    protected String id;
    protected Command.CommandType type;
    protected ActorRef router;
    protected Instruction command;

    private static ObjectMapper mapper = new ObjectMapper();

    public static CreditDebitCommand fromJson(final JsonNode jsonNode) {
        try {
            return mapper.treeToValue(jsonNode, CreditDebitCommand.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public Command.CommandType getType() {
        return type;
    }

    public void setType(final Command.CommandType type) {
        this.type = type;
    }

    public long getValue() {
        return Command.CommandType.DEBIT.equals(type) ? command.value * -1 : command.value;
    }

    public ActorRef getRouter() {
        return router;
    }

    public void setRouter(final ActorRef router) {
        this.router = router;
    }

    public Instruction getCommand() {
        return command;
    }

    public void setCommand(final Instruction command) {
        this.command = command;
    }

    public class Instruction {
        protected String account;
        protected long value;

        public String getAccount() {
            return account;
        }

        public void setAccount(final String account) {
            this.account = account;
        }

        public long getValue() {
            return value;
        }

        public void setValue(final long value) {
            this.value = value;
        }


        @Override
        public String toString() {
            return new StringJoiner(", ", Instruction.class.getSimpleName() + "[", "]")
                    .add("account='" + account + "'")
                    .add("value=" + value)
                    .toString();
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CreditDebitCommand.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("type=" + type)
                .add("router=" + router)
                .add("command=" + command)
                .toString();
    }
}
