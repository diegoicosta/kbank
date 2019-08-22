package kbank.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kbank.gateway.Command;

import java.util.Date;

public class CommandResponse {
    protected String id;
    protected JsonNode response;
    protected JsonNode command;

    private static ObjectMapper mapper = new ObjectMapper();

    public static CommandResponse buildForRead(final JsonNode jsonCommand, final Long value) {
        Command readCommand = mapper.convertValue(jsonCommand, Command.class);
        ObjectNode response = JsonNodeFactory.instance.objectNode();
        response.put("timestamp", new Date().getTime());
        response.put("status", Status.READ_ACCEPTED.toString());
        response.put("balance", value == null ? 0L : value);

        return new CommandResponse(response, readCommand);
    }

    public static CommandResponse buildForChange(final JsonNode jsonCommand, final Status status) {
        Command command = mapper.convertValue(jsonCommand, Command.class);
        ObjectNode response = JsonNodeFactory.instance.objectNode();
        response.put("timestamp", new Date().getTime());
        response.put("status", status.toString());

        return new CommandResponse(response, command);
    }

    private CommandResponse(final JsonNode response, final Command command) {
        this.response = response;
        this.command = command.getCommand();
        command.getType();
        this.id = command.getId();
    }

    public JsonNode getResponse() {
        return response;
    }

    public JsonNode getCommand() {
        return command;
    }

    public String getId() {
        return id;
    }

    public enum Status {
        CHANGE_REFUSED, CHANGE_ACCEPTED, READ_ACCEPTED
    }
}
