/**
 * The body posted to a subscriber's event endpoint, which marks the event as received by returning 200.
 *
 * Fields:
 * * id: Used by the subscriber to mark the event as completed
 * * eventType: The type of the event
 * * message: The event's message, exactly as it was submitted
 */
export interface EventMessage {
  id: string;
  eventType: string;
  message: string;
}

/**
 * What a subscriber's health endpoint responds with.
 *
 * Fields:
 * * isActive: If the subscriber should stay subscribed, when false its subscription is removed
 * * acceptingEvents: If the subscriber can be sent an event now, false means that it is busy
 */
export interface HealthResponse {
  isActive: boolean;
  acceptingEvents: boolean;
}

/**
 * The body posted to the contact address of a producer once a subscriber has received its stream event.
 *
 * Fields:
 * * message: The event's message, exactly as it was submitted
 * * contact: How the producer may contact the subscriber, the producer does not have to
 * * extra-info: The extra info from the subscriber, if there is any
 */
export interface StreamContact {
  message: string;
  contact: string;
  "extra-info"?: string | null;
}

/**
 * What a subscriber's event endpoint must respond with when the mode of the event is stream.
 *
 * Fields:
 * * contactAddress: How the producer may contact the subscriber while it processes the event
 * * extra-info: Passed on to the producer as it is, if there is any
 */
export interface StreamEventResponse {
  contactAddress: string;
  "extra-info"?: string | null;
}

/**
 * A request to be sent every event of one type.
 *
 * The subscriber must be fully set up before it subscribes: the server calls the health endpoint during the request and
 * rejects the subscription unless it responds as active.  Whether it is busy does not matter.
 *
 * Fields:
 * * name: Not unique, just used for debugging
 * * eventType: What event type this subscription is subscribing to
 * * healthEndpoint: Url called with a GET for a `HealthResponse`
 * * eventEndpoint: Url called with a POST of an `EventMessage` for each event
 */
export interface Subscription {
  name: string;
  eventType: string;
  healthEndpoint: string;
  eventEndpoint: string;
}

/**
 * Thrown when a call to the server fails.
 *
 * Fields:
 * * status: The HTTP status that the server returned, or undefined if it never responded
 */
export class StreamQueueError extends Error {
  readonly status: number | undefined;

  /**
   * Creates an error.
   *
   * @param message - What failed.
   * @param options - The HTTP status that the server returned, or what made the call fail if there was no response.
   */
  constructor(message: string, options?: { status?: number; cause?: unknown });
}

/** Calls a daylin-sq server.  Works in browsers and Node. */
export class StreamQueueClient {
  /**
   * Creates a client, without calling the server.
   *
   * @param serverAddress - The address of the server, such as http://localhost:5000
   */
  constructor(serverAddress: string);

  /**
   * Submits an event to be sent to a subscriber.
   *
   * @param eventType - The type of the event.
   * @param message - The message of the event.
   * @param contact - Url called with a POST of a `StreamContact` once a subscriber receives the event, or left out to
   *   not be called.  Only allowed if the mode of the event type is stream.
   * @returns The id of the event.
   * @throws {StreamQueueError} If the call fails.
   */
  submitEvent(eventType: string, message: string, contact?: string): Promise<string>;

  /**
   * Subscribes to an event type.  The subscriber must be fully set up before calling this, the server calls its health
   * endpoint before responding and rejects the subscription if it is not active.  `StreamQueueConsumer` does this.
   *
   * @param subscription - The subscription to add.
   * @returns The id of the subscription.
   * @throws {StreamQueueError} If the call fails or the subscription is rejected.
   */
  subscribe(subscription: Subscription): Promise<string>;

  /**
   * Marks an event as completed, removing it from the server.
   *
   * @param id - The id of the event.
   * @returns False if the event is no longer held.
   * @throws {StreamQueueError} If the call fails.
   */
  completeEvent(id: string): Promise<boolean>;
}
