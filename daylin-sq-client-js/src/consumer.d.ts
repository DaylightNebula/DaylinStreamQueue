import type { EventMessage, StreamEventResponse, StreamQueueClient } from "./client.js";

/**
 * The options of a `StreamQueueConsumer`.
 *
 * Fields:
 * * name: The name of the subscription, not unique and just used for debugging
 * * eventType: The type of event to consume
 * * port: The port to host the endpoints on, any free port is used by default
 * * publicAddress: Finds the address that the server can reach the consumer at from the port that is used, which is
 *   http://localhost:port by default
 * * maxRunning: How many events to consume at once before responding as busy, 1 by default
 * * streamResponse: Finds how the producer may contact the consumer about an event, called before the handler and must
 *   finish within 5 seconds.  Required if the mode of the event type is stream, if this throws then the event is rejected
 *   and the server removes the subscription
 * * handler: Consumes an event, which is completed when this finishes.  If this throws, then the event is sent again
 *   after the server's max processing duration
 */
export interface StreamQueueConsumerOptions {
  name: string;
  eventType: string;
  port?: number;
  publicAddress?: (port: number) => string;
  maxRunning?: number;
  streamResponse?: (event: EventMessage) => StreamEventResponse | Promise<StreamEventResponse>;
  handler: (event: EventMessage) => void | Promise<void>;
}

/** Hosts the endpoints of a subscriber and consumes its events.  Call `start` to begin.  Only works in Node. */
export class StreamQueueConsumer {
  /**
   * Creates a consumer, without starting it.
   *
   * @param client - The server to consume events from.
   * @param options - What to consume and how.
   */
  constructor(client: StreamQueueClient, options: StreamQueueConsumerOptions);

  /**
   * Hosts the endpoints and then subscribes.  If subscribing fails then the endpoints stay up, and `resume` can be called
   * to try again.
   *
   * @returns The id of the subscription.
   * @throws {StreamQueueError} If subscribing fails.
   * @throws {Error} If the consumer was already started or the port cannot be used.
   */
  start(): Promise<string>;

  /** Responds as inactive, so that the server removes the subscription.  Running events are still finished. */
  pause(): void;

  /**
   * Subscribes again, after `pause` or if the server removed the subscription.
   *
   * @returns The id of the new subscription.
   * @throws {StreamQueueError} If subscribing fails.
   * @throws {Error} If the consumer was not started.
   */
  resume(): Promise<string>;

  /**
   * Stops taking events and waits for the running ones to finish.
   *
   * @returns Once every running event is finished.
   */
  close(): Promise<void>;
}
