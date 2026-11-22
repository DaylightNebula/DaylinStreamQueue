package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.HealthResponse
import io.daylightnebula.streamqueue.core.Subscription
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlin.uuid.Uuid

/** The active subscriptions, saved in a [SubscriptionStore] and cached in memory. */
object Subscriptions {

    private val timerTick = 1.seconds
    private val logger = LoggerFactory.getLogger(Subscriptions::class.java)

    private val lock = ReentrantLock()
    private val active = HashMap<String, MutableList<ActiveSubscription>>()
    private lateinit var store: SubscriptionStore

    /**
     * A subscription that events can be sent to.
     *
     * @param id identifies the subscription inside the server
     * @param subscription what the subscriber asked for
     * @property lastHealthCheck when the last health check started, or null if there has not been one since the server
     * started
     */
    class ActiveSubscription(val id: Uuid, val subscription: Subscription) {
        @Volatile
        var lastHealthCheck: TimeSource.Monotonic.ValueTimeMark? = null
    }

    /**
     * Fills the cache with every saved subscription, replacing what it held before.
     *
     * @param store where the subscriptions are saved
     */
    fun init(store: SubscriptionStore) {
        lock.withLock {
            this.store = store
            active.clear()
            store.load().forEach { (id, subscription) -> cache(ActiveSubscription(id, subscription)) }
        }
    }

    /** Starts the timer that health checks every subscription within [Config.Durations.healthCheckInterval]. */
    fun startHealthChecks() {
        thread(isDaemon = true, name = "subscription-health") {
            while (true) {
                runCatching { checkDue() }
                    .onFailure { logger.error("Failed to health check subscriptions", it) }
                Thread.sleep(timerTick.inWholeMilliseconds)
            }
        }
    }

    /**
     * Adds a subscription if its health endpoint responds as active, whether it is busy does not matter. An existing
     * subscription to the same event type with the same health endpoint is ejected.
     *
     * @param subscription the subscription to add
     * @return the id of the new subscription, or null if it is not active
     */
    fun add(subscription: Subscription): Uuid? {
        val entry = ActiveSubscription(Uuid.random(), subscription)
        entry.lastHealthCheck = TimeSource.Monotonic.markNow()
        if (SubscriberClient.checkHealth(subscription.healthEndpoint)?.isActive != true) return null

        lock.withLock {
            val replaced = active[subscription.eventType]
                ?.find { it.subscription.healthEndpoint == subscription.healthEndpoint }
            store.save(entry.id, subscription, replaced?.id)
            replaced?.let { uncache(it) }
            cache(entry)
        }
        logger.info("Added subscription {} {}", entry.id, subscription)
        return entry.id
    }

    /**
     * Removes a subscription, does nothing if it was already removed.
     *
     * @param entry the subscription to remove
     */
    fun remove(entry: ActiveSubscription) {
        val removed = lock.withLock {
            store.remove(entry.id)
            uncache(entry)
        }
        if (removed) logger.info("Removed subscription {} {}", entry.id, entry.subscription)
    }

    /**
     * Lists the active subscriptions to an event type.
     *
     * @param eventType the event type to look up
     * @return a copy of the subscriptions, in the order they should next be offered an event
     */
    fun of(eventType: String): List<ActiveSubscription> = lock.withLock { active[eventType].orEmpty().toList() }

    /**
     * Health checks the subscriptions to an event type until one is active and not busy, then moves it to the back of
     * the round robin. Subscriptions that fail the check are removed.
     *
     * @param eventType the event type to find a subscriber for
     * @return the subscription to send an event to, or null if none can take one
     */
    fun nextAvailable(eventType: String): ActiveSubscription? = of(eventType).find { entry ->
        val available = check(entry.subscription.healthEndpoint, listOf(entry))?.acceptingEvents == true

        // it may have been removed or replaced during the check
        available && lock.withLock { uncache(entry).also { if (it) cache(entry) } }
    }

    /**
     * Health checks every subscription that is due. Subscriptions that share a health endpoint are checked with one
     * call, and event types with a subscriber that can take an event are dispatched.
     */
    fun checkDue() {
        // due early enough that waiting for the next tick does not go over the interval
        val dueAfter = Config.get().durations.healthCheckInterval - timerTick * 2
        val due = lock.withLock { active.values.flatten() }
            .groupBy { it.subscription.healthEndpoint }
            .filterValues { sharing -> sharing.any { it.isDue(dueAfter) } }

        due.forEach { (endpoint, sharing) ->
            // mark now so that a slow check is not started again by the next tick
            markChecked(sharing)
            Thread.startVirtualThread {
                runCatching {
                    val health = check(endpoint, sharing)
                    if (health?.acceptingEvents == true) sharing.forEach { Dispatcher.trigger(it.subscription.eventType) }
                }.onFailure { logger.error("Failed to health check {}", endpoint, it) }
            }
        }
    }

    private fun ActiveSubscription.isDue(dueAfter: Duration): Boolean {
        val last = lastHealthCheck ?: return true
        return last.elapsedNow() >= dueAfter
    }

    /** Returns the health of an endpoint, or null after removing the subscriptions if it is not active. */
    private fun check(endpoint: String, entries: List<ActiveSubscription>): HealthResponse? {
        markChecked(entries)
        val health = SubscriberClient.checkHealth(endpoint)
        if (health?.isActive == true) return health

        entries.forEach { remove(it) }
        return null
    }

    private fun markChecked(entries: List<ActiveSubscription>) {
        val now = TimeSource.Monotonic.markNow()
        entries.forEach { it.lastHealthCheck = now }
    }

    private fun cache(entry: ActiveSubscription) {
        active.getOrPut(entry.subscription.eventType) { mutableListOf() } += entry
    }

    private fun uncache(entry: ActiveSubscription): Boolean =
        active[entry.subscription.eventType]?.remove(entry) == true
}
