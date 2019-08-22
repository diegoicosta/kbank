package kbank.response;

public class ResponseWanted {
    private String commandId;

    public ResponseWanted(final String commandId) {
        this.commandId = commandId;
    }

    public String getCommandId() {
        return commandId;
    }

}
