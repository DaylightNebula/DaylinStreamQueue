package io.daylightnebula.streamqueue

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class DaylinSQApplication

/**
 * Starts the server.
 *
 * @param args passed on to Spring
 */
fun main(args: Array<String>) {
	Database.init()
	Dispatcher.start(Database.stores.keys)
	runApplication<DaylinSQApplication>(*args)

	// subscribers that are sent an event need to be able to call this server back
	Subscriptions.startHealthChecks()
}
