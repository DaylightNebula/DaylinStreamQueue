package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.StreamContact
import org.slf4j.LoggerFactory
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread

/** Sends events to subscribers, with one thread for each event type so that its events are sent in order. */
object Dispatcher {

    private val logger = LoggerFactory.getLogger(Dispatcher::class.java)

    @Volatile
    private var triggers = emptyMap<String, Semaphore>()

    /**
     * Starts the thread of each event type. They stay idle until they are triggered.
     *
     * @param eventTypes the event types to send
     */
    fun start(eventTypes: Set<String>) {
        triggers = eventTypes.associateWith { Semaphore(0) }
        triggers.forEach { (eventType, trigger) ->
            thread(isDaemon = true, name = "dispatch-$eventType") {
                while (true) {
                    // a trigger during dispatch leaves a permit, so that it is never missed
                    trigger.acquire()
                    trigger.drainPermits()
                    runCatching { dispatch(eventType) }
                        .onFailure { logger.error("Failed to dispatch {}", eventType, it) }
                }
            }
        }
    }

    /**
     * Wakes the thread of an event type, called whenever an event may have become sendable. Does nothing if it was not
     * started.
     *
     * @param eventType the event type to send
     */
    fun trigger(eventType: String) {
        triggers[eventType]?.release()
    }

    /**
     * Sends waiting events to subscribers until there are no more events or no subscriber can take one. An event that
     * is sent is marked as received, and if its mode is stream then its producer is told how to contact the subscriber.
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

            if (!send(eventType, event, subscriber.subscription.eventEndpoint)) {
                store.giveBack(event)
                Subscriptions.remove(subscriber)
            }
        }
    }

    /** Returns false if the subscriber did not receive the event. */
    private fun send(eventType: String, event: Database.BorrowedEvent, endpoint: String): Boolean {
        val message = EventMessage(event.id, eventType, event.message)
        if (Config.get().events[eventType]?.mode != Config.Mode.Stream) {
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
