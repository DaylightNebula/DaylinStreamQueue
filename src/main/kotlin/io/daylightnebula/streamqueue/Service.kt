package io.daylightnebula.streamqueue

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
    
    @GetMapping("/health")
    fun health(): String = Config.get().id.toString()

    @PostMapping("/submit-event/{eventType}")
    fun submitEvent(
        @PathVariable("eventType") eventType: String,
        @RequestBody message: String
    ): String {
        if (eventType !in Database.stores) throw ResponseStatusException(HttpStatus.NOT_FOUND)
        val id = Uuid.random()
        Database.push(eventType, id, message)
        return id.toString()
    }

    @PostMapping("/recv-event/{eventId}")
    fun recvEvent(
        @PathVariable("eventId") eventIdString: String
    ) {
        val eventId = Uuid.parseOrNull(eventIdString) ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST)
        if (!Database.markReceived(eventId)) throw ResponseStatusException(HttpStatus.NOT_FOUND)
    }

    @PostMapping("/complete-event/{eventId}")
    fun completeEvent(
        @PathVariable("eventId") eventIdString: String
    ) {
        val eventId = Uuid.parseOrNull(eventIdString) ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST)
        if (!Database.pop(eventId)) throw ResponseStatusException(HttpStatus.NOT_FOUND)
    }
}