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

@Serializable
data class Config(
    val events: Map<String, EventDescription>,
    val id: Uuid = Uuid.random(),
    val durations: Durations = Durations()
) {
    companion object {
        var config: Config? = null
        
        val yaml = Yaml(
            configuration = YamlConfiguration(
                polymorphismStyle = PolymorphismStyle.Property
            )
        )

        fun load(): Config {
            val content = File("config.yml").readText()
            config = yaml.decodeFromString<Config>(content)
            return config!!
        }

        fun get() = config ?: load()
    }

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
        @Serializable @SerialName("Http")
        class Http(val address: String): Consumer()

        @Serializable @SerialName("Command")
        class Command(val command: String): Consumer()
    }

    @Serializable
    data class Durations(
        // once an event is sent to a consumer, how long should we wait for them to acknowledge before unborrowing the event
        val maxNoReceiveDuration: Duration = 10.seconds,
        // once an event is received by a consumer, how long should we wait for them to acknowledge they have completed the event
        val maxProcessingDuration: Duration = 20.minutes,
        // what is the max time an event may exist in the database
        val maxTimeInQueue: Duration = 24.hours
    )
}