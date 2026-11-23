import assert from "node:assert/strict";
import { createServer } from "node:http";
import { afterEach, beforeEach, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { StreamQueueClient, StreamQueueError } from "daylin-sq-client-js";
import { StreamQueueConsumer } from "daylin-sq-client-js/consumer";

const ID = "5f0c9a52-6f0e-4a35-9d8e-0d8b3f1f6f11";
const EVENT = { id: ID, eventType: "test-event", message: "hello" };
const FREE = { isActive: true, acceptingEvents: true };
const BUSY = { isActive: true, acceptingEvents: false };
const INACTIVE = { isActive: false, acceptingEvents: false };

// a fake server that records the calls made to it
let server;
let client;
let calls;
let consumers;
let status;

beforeEach(async () => {
  calls = [];
  consumers = [];
  status = () => 200;
  server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const path = request.url.replace(/^\/api/, "");
    calls.push([path, Buffer.concat(chunks).toString("utf8")]);
    response.writeHead(await status(path)).end(ID);
  });
  await new Promise((resolve) => server.listen(0, resolve));
  client = new StreamQueueClient(`http://localhost:${server.address().port}/`);
});

afterEach(async () => {
  await Promise.all(consumers.map((consumer) => consumer.close()));
  server.close();
  server.closeAllConnections();
});

/** Creates a consumer that is closed after the test. */
function consumer(options = {}) {
  const created = new StreamQueueConsumer(client, {
    name: "test",
    eventType: "test-event",
    handler: () => {},
    ...options
  });
  consumers.push(created);
  return created;
}

/** The last subscription that was made. */
function subscription() {
  return JSON.parse(calls.findLast(([path]) => path === "/subscribe/test-event")[1]);
}

/** Calls the health endpoint of the subscriber. */
async function health() {
  const response = await fetch(subscription().healthEndpoint);
  assert.equal(response.status, 200);
  return response.json();
}

/** Posts a body to the event endpoint of the subscriber. */
async function sendEvent(body = JSON.stringify(EVENT)) {
  const response = await fetch(subscription().eventEndpoint, { method: "POST", body });
  return { status: response.status, body: await response.text() };
}

/** Counts the calls made to a path. */
function count(path) {
  return calls
    .filter(([called]) => called === path)
    .length;
}

/** Waits for a condition to be true. */
async function waitFor(condition) {
  const end = Date.now() + 10_000;
  while (!(await condition())) {
    assert.ok(Date.now() < end, "timed out");
    await sleep(5);
  }
}

/** Waits for the event to be completed. */
function waitForComplete() {
  return waitFor(() => count(`/complete-event/${ID}`) > 0);
}

test("client submits events", async () => {
  assert.equal(await client.submitEvent("test-event", "héllo"), ID);
  assert.deepEqual(calls, [["/submit-event/test-event", "héllo"]]);
});

test("client submits events with an encoded contact address", async () => {
  await client.submitEvent("test-event", "hello", "http://producer:3245/contact?a=b&c=é");
  const query = "contact=http%3A%2F%2Fproducer%3A3245%2Fcontact%3Fa%3Db%26c%3D%C3%A9";
  assert.deepEqual(calls, [[`/submit-event/test-event?${query}`, "hello"]]);
});

test("client returns if an event is still held", async () => {
  assert.equal(await client.completeEvent(ID), true);
  status = () => 404;
  assert.equal(await client.completeEvent(ID), false);
});

test("client throws when a call fails", async () => {
  status = () => 500;
  await assert.rejects(client.submitEvent("test-event", ""), { name: "StreamQueueError", status: 500 });
  await assert.rejects(client.completeEvent(ID), { name: "StreamQueueError", status: 500 });

  server.close();
  server.closeAllConnections();
  await assert.rejects(client.submitEvent("test-event", ""), (err) => {
    assert.ok(err instanceof StreamQueueError);
    assert.equal(err.status, undefined);
    assert.ok(err.cause instanceof Error);
    return true;
  });
});

test("consumer is active before it subscribes", async () => {
  let healthWhenSubscribing;
  status = async () => {
    healthWhenSubscribing = await health();
    return 200;
  };
  assert.equal(await consumer().start(), ID);
  assert.deepEqual(healthWhenSubscribing, FREE);
});

test("consumer cannot start twice or resume before it starts", async () => {
  const created = consumer();
  await assert.rejects(created.resume(), /not started/);
  await created.start();
  await assert.rejects(created.start(), /already started/);
});

test("consumer fails to start on a port that is used", async () => {
  await assert.rejects(consumer({ port: server.address().port }).start(), /Failed to host/);
});

test("consumer handles and completes an event", async () => {
  const handled = [];
  await consumer({ handler: (event) => handled.push(event) }).start();
  assert.deepEqual(await sendEvent(), { status: 200, body: "" });
  await waitForComplete();
  assert.deepEqual(handled, [EVENT]);
});

test("consumer returns its stream response before the event is handled", async () => {
  const finish = Promise.withResolvers();
  await consumer({
    streamResponse: async (event) => ({
      contactAddress: `http://consumer:3244/${event.id}`,
      "extra-info": "info"
    }),
    handler: () => finish.promise
  }).start();

  const response = await sendEvent();
  assert.equal(response.status, 200);
  assert.equal(response.body, `{"contactAddress":"http://consumer:3244/${ID}","extra-info":"info"}`);
  assert.equal(count(`/complete-event/${ID}`), 0);
  finish.resolve();
  await waitForComplete();
});

test("consumer rejects an event when its stream response throws and stays available", async () => {
  let handled = false;
  await consumer({
    streamResponse: () => {
      throw new Error("expected by the test");
    },
    handler: () => {
      handled = true;
    }
  }).start();
  assert.equal((await sendEvent()).status, 500);
  assert.deepEqual(await health(), FREE);
  assert.equal(handled, false);
});

test("consumer is busy until its events are handled and free before they are completed", async () => {
  const finish = Promise.withResolvers();
  let healthWhenCompleting;
  status = async (path) => {
    if (path.startsWith("/complete-event")) healthWhenCompleting = await health();
    return 200;
  };
  await consumer({ maxRunning: 2, handler: () => finish.promise }).start();

  await sendEvent();
  assert.deepEqual(await health(), FREE);
  await sendEvent();
  assert.deepEqual(await health(), BUSY);
  finish.resolve();
  await waitFor(() => healthWhenCompleting !== undefined);
  assert.deepEqual(healthWhenCompleting, FREE);
});

test("consumer does not complete an event when the handler throws and stays available", async () => {
  let started = false;
  await consumer({
    handler: () => {
      started = true;
      throw new Error("expected by the test");
    }
  }).start();
  await sendEvent();
  await waitFor(async () => started && (await health()).acceptingEvents);
  assert.equal(count(`/complete-event/${ID}`), 0);
});

test("closing waits for the running events", async () => {
  const created = consumer({ handler: () => sleep(100) });
  await created.start();
  await sendEvent();
  await created.close();
  assert.equal(count(`/complete-event/${ID}`), 1);
  await assert.rejects(health());
});

test("a paused consumer is inactive until it resumes", async () => {
  const created = consumer();
  await created.start();
  created.pause();
  assert.deepEqual(await health(), INACTIVE);
  assert.equal((await sendEvent()).status, 503);

  await created.resume();
  assert.deepEqual(await health(), FREE);
  assert.equal(count("/subscribe/test-event"), 2);
});

test("invalid events are rejected", async () => {
  await consumer().start();
  assert.equal((await sendEvent("not json")).status, 400);
  assert.equal((await sendEvent("null")).status, 400);
  assert.equal((await sendEvent(`{"id":"${ID}"}`)).status, 400);
  assert.equal((await fetch(subscription().eventEndpoint)).status, 405);
  assert.deepEqual(await health(), FREE);
});

test("a rejected subscription can be tried again", async () => {
  status = () => 422;
  const created = consumer();
  await assert.rejects(created.start(), { status: 422 });
  status = () => 200;
  assert.equal(await created.resume(), ID);
});
