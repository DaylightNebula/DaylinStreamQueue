package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.EventMode
import io.daylightnebula.streamqueue.core.StreamContact
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread

/** Sends events to subscribers, with one thread for each event type so that its events are sent in order. */
object Dispatcher {

    private val logger = LoggerFactory.getLogger(Dispatcher::class.java)

    @Volatile
    private var triggers: ConcurrentHashMap<String, Semaphore>? = null

    /** Lets event types be triggered, the thread of each one is started the first time that it is. */
    fun start() {
        triggers = ConcurrentHashMap()
    }

    /** Stops event types from being triggered. */
    fun stop() {
        triggers = null
    }

    /**
     * Wakes the thread of an event type, called whenever an event may have become sendable. Does nothing if this was not
     * started.
     *
     * @param eventType the event type to send, must be registered
     */
    fun trigger(eventType: String) {
        triggers?.computeIfAbsent(eventType, ::startThread)?.release()
    }

    /** Starts the thread of an event type, which stays idle until the returned semaphore is released. */
    private fun startThread(eventType: String): Semaphore {
        val trigger = Semaphore(0)
        thread(isDaemon = true, name = "dispatch-$eventType") {
            while (true) {
                // a trigger during dispatch leaves a permit, so that it is never missed
                trigger.acquire()
                trigger.drainPermits()
                runCatching { dispatch(eventType) }
                    .onFailure { logger.error("Failed to dispatch {}", eventType, it) }
            }
        }
        return trigger
    }

    /**
     * Sends waiting events to subscribers until there are no more events or no subscriber can take one. An event that
     * is sent is marked as received, and if its mode is response then its producer is told how to contact the subscriber.
     * An event that fails to send goes back to the front of the queue and its subscription is removed.
     *
     * @param eventType the event type to send
     */
    fun dispatch(eventType: String) {
        val store = Database.stores[eventType] ?: return
        val durations = Config.get().durations
        while (store.hasAvailable(durations)) {
            // borrow after finding a subscriber, so that the health checks do not use up its time to be received
            val subscriber = Subscriptions.nextAvailable(eventType) ?: return
            val event = store.borrow(durations) ?: return

            if (!send(eventType, store.mode, event, subscriber.subscription.eventEndpoint)) {
                store.giveBack(event)
                Subscriptions.remove(subscriber)
            }
        }
    }

    /** Returns false if the subscriber did not receive the event. */
    private fun send(eventType: String, mode: EventMode, event: Database.BorrowedEvent, endpoint: String): Boolean {
        val message = EventMessage(event.id, eventType, event.message)
        if (mode == EventMode.Simple) {
            if (!SubscriberClient.sendEvent(endpoint, message)) return false
            Database.markReceived(eventType, event)
            return true
        }

        val response = SubscriberClient.sendStreamEvent(endpoint, message) ?: return false
        Database.markReceived(eventType, event)
        event.contact?.let {
            val contact = StreamContact(event.message, response.contactAddress, response.extraInfo)
            SubscriberClient.contactProducer(it, contact)
        }
        return true
    }
}
