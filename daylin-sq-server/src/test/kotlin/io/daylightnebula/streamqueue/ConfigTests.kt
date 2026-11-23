package io.daylightnebula.streamqueue

import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.writeText
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.time.Duration.Companion.seconds

class ConfigTests {

    @TempDir
    lateinit var folder: Path

    @AfterTest
    fun close() {
        Config.config = null
    }

    private fun load(path: Path) = Config.load(path.toFile())

    @Test
    fun `the default values are used when there is no file`() {
        val config = load(folder.resolve("config.yml"))

        assertEquals(Config.Durations(), config.durations)
        assertEquals(emptyList(), config.corsOrigins)
        assertSame(config, Config.get())
    }

    @Test
    fun `the default values are used when it is a folder`() {
        val config = load(folder.resolve("config.yml").createDirectory())

        assertEquals(Config.Durations(), config.durations)
    }

    @Test
    fun `values that are not in the file are the default`() {
        val file = folder.resolve("config.yml")
        file.writeText("durations:\n  subscriberTimeout: PT2S\n")

        val config = load(file)

        assertEquals(Config.Durations(subscriberTimeout = 2.seconds), config.durations)
        assertEquals(emptyList(), config.corsOrigins)
    }
}
