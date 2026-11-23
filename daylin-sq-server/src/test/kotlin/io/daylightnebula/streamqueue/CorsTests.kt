package io.daylightnebula.streamqueue

import org.springframework.http.HttpHeaders
import org.springframework.mock.web.MockServletContext
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.options
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import kotlin.test.AfterTest
import kotlin.test.Test

class CorsTests {

    @EnableWebMvc
    class Mvc

    private val context = AnnotationConfigWebApplicationContext()

    @AfterTest
    fun close() {
        context.close()
        Config.config = null
    }

    private fun mockMvc(vararg origins: String): MockMvc {
        Config.config = Config(events = emptyMap(), corsOrigins = origins.toList())
        context.servletContext = MockServletContext()
        context.register(Mvc::class.java, Cors::class.java, Service::class.java)
        context.refresh()
        return MockMvcBuilders.webAppContextSetup(context).build()
    }

    private fun MockMvc.getFrom(origin: String) = get("/api/health") { header(HttpHeaders.ORIGIN, origin) }

    @Test
    fun `no origins are allowed by default`() {
        mockMvc().getFrom("https://page.com").andExpect {
            status { isOk() }
            header { doesNotExist(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN) }
        }
    }

    @Test
    fun `only origins in the config are allowed`() {
        val mockMvc = mockMvc("https://page.com", "https://other.com")

        mockMvc.getFrom("https://other.com").andExpect {
            status { isOk() }
            header { string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "https://other.com") }
        }
        mockMvc.getFrom("https://evil.com").andExpect {
            status { isForbidden() }
            header { doesNotExist(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN) }
        }
    }

    @Test
    fun `calls without an origin are still allowed`() {
        mockMvc("https://page.com").get("/api/health").andExpect { status { isOk() } }
    }

    @Test
    fun `any origin can be allowed`() {
        mockMvc("*").getFrom("https://page.com").andExpect {
            status { isOk() }
            header { string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*") }
        }
    }

    @Test
    fun `json can be posted from an allowed origin`() {
        mockMvc("https://page.com").options("/api/subscribe/test-event") {
            header(HttpHeaders.ORIGIN, "https://page.com")
            header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
            header(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
        }.andExpect {
            status { isOk() }
            header { string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "https://page.com") }
            header { string(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, "content-type") }
        }
    }
}
