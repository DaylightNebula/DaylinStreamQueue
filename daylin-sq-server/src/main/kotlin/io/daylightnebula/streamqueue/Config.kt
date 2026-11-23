package io.daylightnebula.streamqueue

import com.charleskorn.kaml.PolymorphismStyle
import com.charleskorn.kaml.Yaml
import com.charleskorn.kaml.YamlConfiguration
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import java.io.File
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * The config of the server.
 *
 * @param events describes each event type by name
 * @param id identifies the server
 * @param durations the time limits used by the server
 * @param corsOrigins the origins of the web pages that may call the server from a browser, or * for any of them
 */
@Serializable
data class Config(
    val events: Map<String, EventDescription>,
    val id: Uuid = Uuid.random(),
    val durations: Durations = Durations(),
    val corsOrigins: List<String> = emptyList()
) {
    companion object {
        var config: Config? = null
        
        val yaml = Yaml(
            configuration = YamlConfiguration(
                polymorphismStyle = PolymorphismStyle.Property
            )
        )

        /**
         * Reads the config from config.yml and caches it.
         *
         * @return the config that was read
         */
        fun load(): Config {
            val content = File("config.yml").readText()
            config = yaml.decodeFromString<Config>(content)
            return config!!
        }

        /**
         * Gets the cached config, reading it first if needed.
         *
         * @return the config
         */
        fun get() = config ?: load()
    }

    /**
     * Describes an event type.
     *
     * @param mode the mode the event is handled with
     * @param consumers where the event is sent
     */
    @Serializable
    data class EventDescription(
        val mode: Mode,
        val consumers: List<Consumer>
    )

    enum class Mode {
        Simple,
        Stream
    }

    @Serializable
    sealed class Consumer {
        /**
         * A consumer that is called over HTTP.
         *
         * @param address the url to call
         */
        @Serializable @SerialName("Http")
        class Http(val address: String): Consumer()
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