// Needs a server on port 5000 that uses daylin-sq-server/config.yml, and the com.daylin:daylin-sq-client-java dependency.

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.daylightnebula.streamqueue.client.StreamQueueClient;
import io.daylightnebula.streamqueue.client.StreamQueueConsumer;
import io.daylightnebula.streamqueue.core.StreamContact;
import io.daylightnebula.streamqueue.core.StreamEventResponse;
import io.daylightnebula.streamqueue.core.StreamQueueJsonKt;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kotlin.Unit;

/**
 * Consumes and submits a simple event, then streams the alphabet from the consumer to the producer of a stream event.
 *
 * Fields:
 * <ul>
 *   <li>STREAM_PORT: The port that the consumer hosts its stream on</li>
 *   <li>CONTACT_PORT: The port that the producer is contacted on</li>
 * </ul>
 */
public class Example {

    private static final int STREAM_PORT = 3244;
    private static final int CONTACT_PORT = 3245;

    /**
     * Runs both examples.
     *
     * @param args not used
     * @throws Exception if either example fails
     */
    public static void main(String[] args) throws Exception {
        StreamQueueClient client = new StreamQueueClient("http://localhost:5000");
        simpleEvent(client);
        streamEvent(client);
    }

    /** Consumes one test event that prints its message. */
    private static void simpleEvent(StreamQueueClient client) throws InterruptedException {
        CountDownLatch handled = new CountDownLatch(1);
        StreamQueueConsumer consumer = new StreamQueueConsumer(
            client,
            "example",
            "test-event",
            0, // any free port
            port -> "http://localhost:" + port,
            1, // how many events to consume at once
            null, // only used by stream events
            event -> {
                System.out.println("Received: " + event.getMessage());
                handled.countDown();
                return Unit.INSTANCE;
            }
        );

        // submit an event and wait for the consumer to handle it
        try (consumer) {
            consumer.start();
            client.submitEvent("test-event", "Hello from Java", null);
            handled.await();
        }
    }

    /** Consumes one alphabet event, whose producer reads the alphabet from the consumer. */
    private static void streamEvent(StreamQueueClient client) throws Exception {
        // consumer: host the stream, this can be anything that the producer knows how to contact
        HttpServer streams = host(STREAM_PORT, exchange -> {
            exchange.sendResponseHeaders(200, 0);
            for (char letter = 'A'; letter <= 'Z'; letter++)
                exchange.getResponseBody().write((letter + "\n").getBytes(StandardCharsets.UTF_8));
        });

        // consumer: tell the producer where the stream is, the handler could keep working on the event while it is read
        StreamQueueConsumer consumer = new StreamQueueConsumer(
            client,
            "example",
            "alphabet",
            0,
            port -> "http://localhost:" + port,
            1,
            event -> new StreamEventResponse(
                "http://localhost:" + STREAM_PORT + "/alphabet/" + event.getId(),
                "one letter on each line"
            ),
            event -> {
                System.out.println("Consuming " + event.getId());
                return Unit.INSTANCE;
            }
        );

        // producer: host the endpoint that is told how to contact the consumer
        CompletableFuture<StreamContact> contacted = new CompletableFuture<>();
        HttpServer contacts = host(CONTACT_PORT, exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            contacted.complete(
                StreamQueueJsonKt
                    .getStreamQueueJson()
                    .decodeFromString(StreamContact.Companion.serializer(), body)
            );
            exchange.sendResponseHeaders(200, -1);
        });

        // producer: submit the event, then read the stream
        try (consumer) {
            consumer.start();
            client.submitEvent("alphabet", "the ABC's please", "http://localhost:" + CONTACT_PORT + "/contact");
            StreamContact contact = contacted.get(30, TimeUnit.SECONDS);
            System.out.println("Streaming from " + contact.getContact() + " with " + contact.getExtraInfo());
            HttpRequest request = HttpRequest
                .newBuilder(URI.create(contact.getContact()))
                .build();
            HttpResponse<String> response = HttpClient
                .newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());
        } finally {
            streams.stop(0);
            contacts.stop(0);
        }
    }

    /** Hosts a server. */
    private static HttpServer host(int port, HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                handler.handle(exchange);
            }
        });
        server.start();
        return server;
    }
}
