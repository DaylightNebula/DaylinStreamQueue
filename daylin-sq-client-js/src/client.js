const TIMEOUT_MS = 30_000;

/**
 * Thrown when a call to the server fails.
 *
 * Fields:
 * * status: The HTTP status that the server returned, or undefined if it never responded
 */
export class StreamQueueError extends Error {
  /**
   * Creates an error.
   *
   * @param {string} message - What failed.
   * @param {{ status?: number, cause?: unknown }} [options] - The HTTP status that the server returned, or what made the
   *   call fail if there was no response.
   */
  constructor(message, { status, cause } = {}) {
    super(message, { cause });
    this.name = "StreamQueueError";
    this.status = status;
  }
}

/**
 * Calls a daylin-sq server.  Works in browsers and Node.
 *
 * Fields:
 * * #api: The address that every call is under
 */
export class StreamQueueClient {
  #api;

  /**
   * Creates a client, without calling the server.
   *
   * @param {string} serverAddress - The address of the server, such as http://localhost:5000
   */
  constructor(serverAddress) {
    this.#api = `${serverAddress.replace(/\/+$/, "")}/api`;
  }

  /**
   * Submits an event to be sent to a subscriber.
   *
   * @param {string} eventType - The type of the event.
   * @param {string} message - The message of the event.
   * @param {string} [contact] - Url called with a POST of a `StreamContact` once a subscriber receives the event, or left
   *   out to not be called.  Only allowed if the mode of the event type is response.
   * @returns {Promise<string>} The id of the event.
   * @throws {StreamQueueError} If the call fails.
   */
  async submitEvent(eventType, message, contact) {
    const query = contact === undefined ? "" : `?contact=${encodeURIComponent(contact)}`;
    const path = `/submit-event/${encodeURIComponent(eventType)}${query}`;
    return this.#post(path, message, "text/plain; charset=utf-8");
  }

  /**
   * Subscribes to an event type.  The subscriber must be fully set up before calling this, the server calls its health
   * endpoint before responding and rejects the subscription if it is not active.  `StreamQueueConsumer` does this.
   *
   * @param {import("./client.js").Subscription} subscription - The subscription to add.
   * @returns {Promise<string>} The id of the subscription.
   * @throws {StreamQueueError} If the call fails or the subscription is rejected.
   */
  async subscribe(subscription) {
    const path = `/subscribe/${encodeURIComponent(subscription.eventType)}`;
    return this.#post(path, JSON.stringify(subscription), "application/json");
  }

  /**
   * Marks an event as completed, removing it from the server.
   *
   * @param {string} id - The id of the event.
   * @returns {Promise<boolean>} False if the event is no longer held.
   * @throws {StreamQueueError} If the call fails.
   */
  async completeEvent(id) {
    try {
      await this.#post(`/complete-event/${encodeURIComponent(id)}`, "", "text/plain");
      return true;
    } catch (err) {
      if (err instanceof StreamQueueError && err.status === 404) return false;
      throw err;
    }
  }

  /** Posts to the server and returns the body of its response. */
  async #post(path, body, contentType) {
    const url = this.#api + path;
    let response;
    let text;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": contentType },
        body,
        signal: AbortSignal.timeout(TIMEOUT_MS)
      });
      text = await response.text();
    } catch (err) {
      throw new StreamQueueError(`Failed to call ${url}`, { cause: err });
    }

    if (!response.ok) {
      throw new StreamQueueError(`${url} returned ${response.status}: ${text}`, { status: response.status });
    }
    return text;
  }
}
