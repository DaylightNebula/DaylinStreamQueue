package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMode
import io.daylightnebula.streamqueue.core.Subscription
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
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
     * @param eventType the type of the event, not found if nothing has ever subscribed to it
     * @param message the message of the event
     * @param contact where the producer may be told how to contact the subscriber, a bad request if it is not a valid
     * url or the mode of the event type is not response
     * @return the id of the event
     */
    @PostMapping("/submit-event/{eventType}")
    fun submitEvent(
        @PathVariable("eventType") eventType: String,
        @RequestBody message: String,
        @RequestParam("contact", required = false) contact: String?
    ): String {
        val store = Database.stores[eventType] ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        if (contact != null && store.mode != EventMode.Response)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Only response events can have a contact address")
        if (contact != null && !SubscriberClient.isValidEndpoint(contact))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The contact address is not a valid url")

        val id = Uuid.random()
        Database.push(eventType, id, message, contact)
        Dispatcher.trigger(eventType)
        return id.toString()
    }

    /**
     * Subscribes to an event type. The subscriber must be fully set up before calling this, its health endpoint is
     * called during the request and must respond as active (busy does not matter). An older subscription to the same
     * event type with the same health endpoint is ejected. The event type is registered with the mode of the first
     * subscription to it.
     *
     * @param eventType the event type to subscribe to, a bad request if it is used by the server
     * @param subscription the subscription, a bad request if its event type is different or an endpoint is not a valid
     * url, unprocessable if it is not active, and a conflict if its event type is registered with another mode
     * @return the id of the subscription
     */
    @PostMapping("/subscribe/{eventType}")
    fun subscribe(
        @PathVariable("eventType") eventType: String,
        @RequestBody subscription: Subscription
    ): String {
        if (Database.isReserved(eventType))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The event type is used by the server")
        if (subscription.eventType != eventType)
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The event type must match the path")
        if (!SubscriberClient.isValidEndpoint(subscription.healthEndpoint))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The health endpoint is not a valid url")
        if (!SubscriberClient.isValidEndpoint(subscription.eventEndpoint))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "The event endpoint is not a valid url")

        val added = try {
            Subscriptions.add(subscription)
        } catch (e: ModeConflictException) {
            throw ResponseStatusException(HttpStatus.CONFLICT, e.message, e)
        }
        val id = added
            ?: throw ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT, "The health endpoint is not active")
        Dispatcher.trigger(eventType)
        return id.toString()
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