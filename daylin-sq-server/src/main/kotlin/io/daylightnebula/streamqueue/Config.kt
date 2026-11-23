package io.daylightnebula.streamqueue

import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import java.io.File
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * The config of the server, every value is optional.
 *
 * @param id identifies the server
 * @param durations the time limits used by the server
 * @param corsOrigins the origins of the web pages that may call the server from a browser, or * for any of them
 */
@Serializable
data class Config(
    val id: Uuid = Uuid.random(),
    val durations: Durations = Durations(),
    val corsOrigins: List<String> = emptyList()
) {
    companion object {
        var config: Config? = null
        
        val yaml = Yaml.default

        /**
         * Reads the config and caches it.
         *
         * @param file the file to read, the default values are used if it is not a file
         * @return the config that was read
         */
        fun load(file: File = File("config.yml")): Config {
            val loaded = if (file.isFile) yaml.decodeFromString<Config>(file.readText()) else Config()
            config = loaded
            return loaded
        }

        /**
         * Gets the cached config, reading it first if needed.
         *
         * @return the config
         */
        fun get() = config ?: load()
    }

    /**
     * The time limits used by the server.
     *
     * @param maxNoReceiveDuration once an event is sent to a consumer, how long should we wait for them to receive it
     * by returning 200 before unborrowing the event
     * @param maxProcessingDuration once an event is received by a consumer, how long should we wait for them to
     * acknowledge they have completed the event
     * @param maxTimeInQueue what is the max time an event may exist in the database
     * @param healthCheckInterval what is the max time between health checks of a subscription
     * @param subscriberTimeout how long should we wait for a subscriber to respond, this should be less than
     * maxNoReceiveDuration
     */
    @Serializable
    data class Durations(
        val maxNoReceiveDuration: Duration = 10.seconds,
        val maxProcessingDuration: Duration = 20.minutes,
        val maxTimeInQueue: Duration = 24.hours,
        val healthCheckInterval: Duration = 30.seconds,
        val subscriberTimeout: Duration = 5.seconds
    )
}