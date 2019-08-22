package kbank;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import kbank.account.AccountWriteStream;
import kbank.gateway.Command;
import kbank.gateway.CommandActorHandler;
import kbank.gateway.CommandKafkaProducer;
import kbank.gateway.CommandStream;
import kbank.response.CommandResponse;
import kbank.response.ResponseActorRouter;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.PathMatchers.segment;
import static akka.pattern.Patterns.ask;

public class RestHttpServer extends AllDirectives {

    private static ActorSystem system;
    private static CommandKafkaProducer producer;

    private ActorRef responseRouter;
    private final LoggingAdapter log = Logging.getLogger(system, this);

    public static void main(String[] args) throws IOException {
        system = ActorSystem.create("kbank-system");
        producer = new CommandKafkaProducer();

        try {
            startHttpServer();
        } finally {
            system.terminate();
        }
    }

    private static void startHttpServer() throws IOException {
        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        RestHttpServer app = new RestHttpServer();

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow =
                app
                        .createRoute()
                        .flow(system, materializer);

        final CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow,
                ConnectHttp.toHost("localhost", 8080), materializer);

        System.out.println("Server online at http://localhost:8080/\nPress RETURN to stop...");
        System.in.read();

        binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(unbound -> system.terminate());
    }

    public RestHttpServer() {
        responseRouter = system.actorOf(ResponseActorRouter.props(), "response-router");
        new CommandStream(responseRouter);
        new AccountWriteStream(responseRouter);
    }

    private Route createRoute() {
        Duration timeout = Duration.ofSeconds(10l);

        return concat(
                pathPrefix("command", () ->
                        post(() -> entity(Jackson.unmarshaller(Command.class), command -> {
                                    ActorRef actorHandler = system.actorOf(CommandActorHandler.props(command.getId(), producer), command.getId());
                                    command.setRouter(responseRouter);
                                    CompletionStage<CommandResponse> resp =
                                            ask(actorHandler, command, timeout)
                                                    .thenApply(CommandResponse.class::cast);

                                    return completeOKWithFuture(resp, Jackson.marshaller());
                                }
                        ))
                ),
                pathPrefix("account", () ->
                        path(segment(), (
                                String accountId) ->
                                get(() ->
                                {
                                    Command command = Command.asReadCommand(accountId);
                                    ActorRef actorHandler = system.actorOf(CommandActorHandler.props(command.getId(), producer), command.getId());
                                    command.setRouter(responseRouter);
                                    CompletionStage<CommandResponse> resp =
                                            ask(actorHandler, command, timeout)
                                                    .thenApply(CommandResponse.class::cast);

                                    return completeOKWithFuture(resp, Jackson.marshaller());
                                })
                        )));
    }


}
