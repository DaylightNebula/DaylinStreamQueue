package io.daylightnebula.streamqueue

import kotlinx.serialization.SerializationException
import kotlinx.serialization.decodeFromString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class ConfigTests {

    private fun decode(consumer: String): Config = Config.yaml.decodeFromString(
        """
        events:
          test-event:
            mode: Stream
            consumers:
              - $consumer
        """.trimIndent()
    )

    @Test
    fun `stream events and http consumers can be configured`() {
        val event = decode("{type: Http, address: \"http://localhost\"}").events.getValue("test-event")

        assertEquals(Config.Mode.Stream, event.mode)
        assertEquals("http://localhost", assertIs<Config.Consumer.Http>(event.consumers.single()).address)
    }

    @Test
    fun `command consumers are rejected`() {
        assertFailsWith<SerializationException> { decode("{type: Command, command: \"ls\"}") }
    }
}
