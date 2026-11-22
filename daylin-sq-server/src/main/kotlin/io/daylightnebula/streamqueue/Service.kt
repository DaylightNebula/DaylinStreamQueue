package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.Subscription
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import kotlin.uuid.Uuid

@RestController
@RequestMapping("/api")
class Service {
    
    /**
     * Shows that the server is running.
     *
     * @return the id of the server from its config
     */
    @GetMapping("/health")
    fun health(): String = Config.get().id.toString()

    /**
     * Adds an event to the back of its queue.
     *
     * @param eventType the type of the event, not found if it is not in the config
     * @param message the message of the event
     * @return the id of the event
     */
    @PostMapping("/submit-event/{eventType}")
    fun submitEvent(
        @PathVariable("eventType") eventType: String,
        @RequestBody message: String
    ): String {
        if (eventType !in Database.stores) throw ResponseStatusException(HttpStatus.NOT_FOUND)
        val id = Uuid.random()
        Database.push(eventType, id, message)
        Dispatcher.trigger(eventType)
        return id.toString()
    }

    /**
     * Subscribes to an event type. The subscriber must be fully set up before calling this, its health endpoint is
     * called during the request and must respond as active (busy does not matter). An older subscription to the same
     * event type with the same health endpoint is ejected.
     *
     * @param eventType the event type to subscribe to, not found if it is not in the config
     * @param subscription the subscription, a bad request if its event type is different or an endpoint is not a valid
     * url, and unprocessable if it is not active
     * @return the id of the subscription
     */
    @PostMapping("/subscribe/{eventType}")
    fun subscribe(
        @PathVariable("eventType") eventType: String,
        @RequestBody subscription: Subscription
    ): String {
        if (eventType !in Database.stores) throw ResponseStatusException(HttpStatus.NOT_FOUND)
        if (subscription.eventType != eventType)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The event type must match the path")
        if (!SubscriberClient.isValidEndpoint(subscription.healthEndpoint))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The health endpoint is not a valid url")
        if (!SubscriberClient.isValidEndpoint(subscription.eventEndpoint))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The event endpoint is not a valid url")

        val id = Subscriptions.add(subscription)
            ?: throw ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT, "The health endpoint is not active")
        Dispatcher.trigger(eventType)
        return id.toString()
    }

    /**
     * Marks an event as received by its consumer.
     *
     * @param eventIdString the id of the event, a bad request if it is not an id and not found if it is not held
     */
    @PostMapping("/recv-event/{eventId}")
    fun recvEvent(
        @PathVariable("eventId") eventIdString: String
    ) {
        val eventId = Uuid.parseOrNull(eventIdString) ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST)
        if (!Database.markReceived(eventId)) throw ResponseStatusException(HttpStatus.NOT_FOUND)
    }

    /**
     * Removes an event that its consumer has finished.
     *
     * @param eventIdString the id of the event, a bad request if it is not an id and not found if it is not held
     */
    @PostMapping("/complete-event/{eventId}")
    fun completeEvent(
        @PathVariable("eventId") eventIdString: String
    ) {
        val eventId = Uuid.parseOrNull(eventIdString) ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST)
        val eventType = Database.complete(eventId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        Dispatcher.trigger(eventType)
    }
}