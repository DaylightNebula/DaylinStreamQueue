package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMessage
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
     * fails to send goes back to the front of the queue and its subscription is removed.
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

            val message = EventMessage(event.id, eventType, event.message)
            if (!SubscriberClient.sendEvent(subscriber.subscription.eventEndpoint, message)) {
                store.giveBack(event)
                Subscriptions.remove(subscriber)
            }
        }
    }
}
