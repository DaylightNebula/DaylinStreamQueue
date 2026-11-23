// Consumes and submits a simple event, then streams the alphabet from the consumer to the producer of a stream event.
//
// Needs a server on port 5000 that uses daylin-sq-server/config.yml, and the daylin-sq-client-js package.

import { createServer } from "node:http";
import { json } from "node:stream/consumers";
import { StreamQueueClient } from "daylin-sq-client-js";
import { StreamQueueConsumer } from "daylin-sq-client-js/consumer";

const STREAM_PORT = 3244;
const CONTACT_PORT = 3245;

/** Consumes one test event that prints its message. */
async function simpleEvent(client) {
  const handled = Promise.withResolvers();
  const consumer = new StreamQueueConsumer(client, {
    name: "example",
    eventType: "test-event",
    handler: (event) => {
      console.log(`Received: ${event.message}`);
      handled.resolve();
    }
  });
  await consumer.start();

  // submit an event and wait for the consumer to handle it
  await client.submitEvent("test-event", "Hello from JavaScript");
  await handled.promise;
  await consumer.close();
}

/** Consumes one alphabet event, whose producer reads the alphabet from the consumer. */
async function streamEvent(client) {
  // consumer: host the stream, this can be anything that the producer knows how to contact
  const streams = await host(STREAM_PORT, (request, response) => {
    for (const letter of "ABCDEFGHIJKLMNOPQRSTUVWXYZ") response.write(`${letter}\n`);
    response.end();
  });

  // consumer: tell the producer where the stream is, the handler could keep working on the event while it is read
  const consumer = new StreamQueueConsumer(client, {
    name: "example",
    eventType: "alphabet",
    streamResponse: (event) => ({
      contactAddress: `http://localhost:${STREAM_PORT}/alphabet/${event.id}`,
      "extra-info": "one letter on each line"
    }),
    handler: (event) => console.log(`Consuming ${event.id}`)
  });
  await consumer.start();

  // producer: host the endpoint that is told how to contact the consumer
  const contacted = Promise.withResolvers();
  const contacts = await host(CONTACT_PORT, (request, response) => {
    json(request).then(contacted.resolve, contacted.reject);
    response.end();
  });

  // producer: submit the event, then read the stream
  await client.submitEvent("alphabet", "the ABC's please", `http://localhost:${CONTACT_PORT}/contact`);
  const contact = await contacted.promise;
  console.log(`Streaming from ${contact.contact} with ${contact["extra-info"]}`);
  const response = await fetch(contact.contact);
  console.log(await response.text());

  await consumer.close();
  streams.close();
  contacts.close();
}

/** Hosts a server. */
async function host(port, listener) {
  const server = createServer(listener);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, () => resolve());
  });
  return server;
}

const client = new StreamQueueClient("http://localhost:5000");
await simpleEvent(client);
await streamEvent(client);
