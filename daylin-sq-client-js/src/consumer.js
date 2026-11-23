import { createServer } from "node:http";

/**
 * Hosts the endpoints of a subscriber and consumes its events.  Call `start` to begin.  Only works in Node.
 *
 * Fields:
 * * #client: The server to consume events from
 * * #options: What to consume and how, see `StreamQueueConsumerOptions`
 * * #server: Hosts the endpoints, once started
 * * #active: If events are taken
 * * #running: How many events are being consumed
 * * #tasks: The promises of the events being consumed and completed
 */
export class StreamQueueConsumer {
  #client;
  #options;
  #server;
  #active = false;
  #running = 0;
  #tasks = new Set();

  /**
   * Creates a consumer, without starting it.
   *
   * @param {import("./client.js").StreamQueueClient} client - The server to consume events from.
   * @param {import("./consumer.js").StreamQueueConsumerOptions} options - What to consume and how.
   */
  constructor(client, options) {
    this.#client = client;
    this.#options = {
      ...options,
      port: options.port ?? 0,
      publicAddress: options.publicAddress ?? ((port) => `http://localhost:${port}`),
      maxRunning: options.maxRunning ?? 1
    };
  }

  /**
   * Hosts the endpoints and then subscribes.  If subscribing fails then the endpoints stay up, and `resume` can be called
   * to try again.
   *
   * @returns {Promise<string>} The id of the subscription.
   * @throws {import("./client.js").StreamQueueError} If subscribing fails.
   * @throws {Error} If the consumer was already started or the port cannot be used.
   */
  async start() {
    if (this.#server !== undefined) throw new Error("The consumer was already started");
    const server = createServer((request, response) => {
      this.#route(request, response).catch((err) => {
        console.error(`Failed to respond to ${request.url}`, err);
        response.destroy();
      });
    });
    this.#server = server;

    try {
      await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(this.#options.port, resolve);
      });
    } catch (err) {
      this.#server = undefined;
      throw new Error(`Failed to host the consumer on port ${this.#options.port}`, { cause: err });
    }
    return this.resume();
  }

  /** Responds as inactive, so that the server removes the subscription.  Running events are still finished. */
  pause() {
    this.#active = false;
  }

  /**
   * Subscribes again, after `pause` or if the server removed the subscription.
   *
   * @returns {Promise<string>} The id of the new subscription.
   * @throws {import("./client.js").StreamQueueError} If subscribing fails.
   * @throws {Error} If the consumer was not started.
   */
  async resume() {
    const bound = this.#server?.address();
    if (!bound) throw new Error("The consumer was not started");
    const address = this.#options.publicAddress(bound.port).replace(/\/+$/, "");

    // the server checks that this is active before it accepts the subscription
    this.#active = true;
    return this.#client.subscribe({
      name: this.#options.name,
      eventType: this.#options.eventType,
      mode: this.#options.streamResponse ? "Response" : "Simple",
      healthEndpoint: `${address}/health`,
      eventEndpoint: `${address}/event`
    });
  }

  /**
   * Stops taking events and waits for the running ones to finish.
   *
   * @returns {Promise<void>} Once every running event is finished.
   */
  async close() {
    this.#active = false;
    const server = this.#server;
    if (server?.listening) {
      await new Promise((resolve) => {
        server.close(resolve);
        server.closeIdleConnections();
      });
    }
    await Promise.all(this.#tasks);
  }

  /** Finds the endpoint of a request. */
  async #route(request, response) {
    const path = request.url.split("?")[0];
    if (path === "/health") return this.#health(response);
    if (path === "/event") return this.#event(request, response);
    respond(response, 404);
  }

  /** The health endpoint. */
  #health(response) {
    const acceptingEvents = this.#active && this.#running < this.#options.maxRunning;
    respond(response, 200, { isActive: this.#active, acceptingEvents });
  }

  /** The event endpoint. */
  async #event(request, response) {
    if (request.method !== "POST") return respond(response, 405);
    const body = await readBody(request);
    if (!this.#active) return respond(response, 503);

    let event;
    try {
      event = parseEvent(body);
    } catch (err) {
      console.warn("Received an invalid event", err);
      return respond(response, 400);
    }

    let streamResponse;
    try {
      streamResponse = await this.#options.streamResponse?.(event);
    } catch (err) {
      console.error(`Failed to find the stream response of event ${event.id}`, err);
      return respond(response, 500);
    }

    // count before responding, so that the server's next health check sees it
    this.#running++;
    const task = this.#consume(event);
    this.#tasks.add(task);
    task.finally(() => this.#tasks.delete(task));
    respond(response, 200, streamResponse);
  }

  /** Handles an event and then completes it, this never rejects. */
  async #consume(event) {
    try {
      // after responding, as the start of the handler could block
      await new Promise((resolve) => setImmediate(resolve));
      try {
        await this.#options.handler(event);
      } finally {
        // before completing, as that makes the server check if this can take another event
        this.#running--;
      }
      const held = await this.#client.completeEvent(event.id);
      if (!held) console.warn(`Event ${event.id} is no longer held by this consumer`);
    } catch (err) {
      console.error(`Failed to consume event ${event.id}`, err);
    }
  }
}

/** Reads the whole body of a request. */
async function readBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

/** Parses an event and checks its fields. */
function parseEvent(body) {
  const event = JSON.parse(body);
  for (const field of ["id", "eventType", "message"]) {
    if (typeof event?.[field] !== "string") throw new TypeError(`The event has no ${field}`);
  }
  return event;
}

/** Responds with JSON, or with no body. */
function respond(response, status, body) {
  if (body === undefined) {
    response.writeHead(status).end();
    return;
  }
  response
    .writeHead(status, { "Content-Type": "application/json" })
    .end(JSON.stringify(body));
}
