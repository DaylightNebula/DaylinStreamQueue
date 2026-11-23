package io.daylightnebula.streamqueue

import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.CorsRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

/** Lets web pages from [Config.corsOrigins] call the server. */
@Configuration
class Cors : WebMvcConfigurer {

    /**
     * Allows the origins in the config to call the api, does nothing if there are none.
     *
     * @param registry where the allowed origins are added
     */
    override fun addCorsMappings(registry: CorsRegistry) {
        val origins = Config.get().corsOrigins
        if (origins.isNotEmpty()) registry.addMapping("/api/**").allowedOrigins(*origins.toTypedArray())
    }
}
