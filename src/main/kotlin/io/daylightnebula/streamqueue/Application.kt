package io.daylightnebula.streamqueue

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class DaylinSQApplication

fun main(args: Array<String>) {
	Database.init()
	runApplication<DaylinSQApplication>(*args)
}
