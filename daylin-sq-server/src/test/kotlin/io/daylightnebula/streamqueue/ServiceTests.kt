package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMode
import org.junit.jupiter.api.io.TempDir
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteOptions
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.ResultActionsDsl
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.nio.file.Path
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.uuid.Uuid

/** Uses a temporary folder for every test, so the real data folder is never touched. */
class ServiceTests {

    @TempDir
    lateinit var folder: Path

    private lateinit var db: RocksDB

    private val mockMvc = MockMvcBuilders.standaloneSetup(Service()).build()
    private val durations = Config.Durations(20.minutes, 20.minutes, 24.hours)

    @BeforeTest
    fun open() {
        RocksDB.loadLibrary()
        Database.writeOptions = WriteOptions()
        db = RocksDB.open(Options().setCreateIfMissing(true), folder.toString())
        useMode(EventMode.Simple)
        Config.config = Config(durations = durations)
    }

    @AfterTest
    fun close() {
        db.close()
        Database.stores = emptyMap()
        Config.config = null
    }

    private fun useMode(mode: EventMode) {
        Database.stores = mapOf("test-event" to Database.EventStore(db, db.defaultColumnFamily, mode))
    }

    private fun post(message: String, contact: String?): ResultActionsDsl =
        mockMvc.post("/api/submit-event/test-event") {
            contentType = MediaType.TEXT_PLAIN
            content = message
            contact?.let { queryParam("contact", it) }
        }

    private fun submit(message: String, contact: String? = null): Uuid = post(message, contact)
        .andExpect { status { isOk() } }
        .andReturn().response.contentAsString.let { Uuid.parse(it) }

    @Test
    fun `health returns the config id`() {
        mockMvc.get("/api/health").andExpect {
            status { isOk() }
            content { string(Config.get().id.toString()) }
        }
    }

    @Test
    fun `a submitted event can be borrowed`() {
        val id = submit("hello")

        val event = assertNotNull(Database.borrow("test-event"))
        assertEquals(id, event.id)
        assertEquals("hello", event.message)
    }

    @Test
    fun `a submitted event has no contact address by default`() {
        submit("hello")

        assertNull(assertNotNull(Database.borrow("test-event")).contact)
    }

    @Test
    fun `a response event can be submitted with a contact address`() {
        useMode(EventMode.Response)
        submit("hello", "http://producer:3245/contact?a=b&c=d")

        val event = assertNotNull(Database.borrow("test-event"))
        assertEquals("hello", event.message)
        assertEquals("http://producer:3245/contact?a=b&c=d", event.contact)
    }

    @Test
    fun `a response event can be submitted without a contact address`() {
        useMode(EventMode.Response)
        submit("hello")

        assertNull(assertNotNull(Database.borrow("test-event")).contact)
    }

    @Test
    fun `invalid contact addresses are bad requests`() {
        useMode(EventMode.Response)
        listOf("", "not a url", "localhost", "ftp://localhost/contact", "http://").forEach {
            post("hello", it).andExpect { status { isBadRequest() } }
        }

        assertNull(Database.borrow("test-event"))
    }

    @Test
    fun `a simple event cannot be submitted with a contact address`() {
        useMode(EventMode.Simple)
        post("hello", "http://producer/contact").andExpect { status { isBadRequest() } }

        assertNull(Database.borrow("test-event"))
    }

    @Test
    fun `there is no endpoint to mark an event as received`() {
        val id = submit("hello")
        Database.borrow("test-event")

        mockMvc.post("/api/recv-event/$id").andExpect { status { isNotFound() } }
        mockMvc.post("/api/complete-event/$id").andExpect { status { isOk() } }
    }

    @Test
    fun `complete-event removes a received event`() {
        val id = submit("hello")
        val event = assertNotNull(Database.borrow("test-event"))
        Database.markReceived("test-event", event)

        mockMvc.post("/api/complete-event/$id").andExpect { status { isOk() } }

        assertFalse(Database.pop("test-event", event))
        mockMvc.post("/api/complete-event/$id").andExpect { status { isNotFound() } }
    }

    @Test
    fun `complete-event removes a borrowed event that was never received`() {
        val id = submit("hello")
        val event = assertNotNull(Database.borrow("test-event"))

        mockMvc.post("/api/complete-event/$id").andExpect { status { isOk() } }

        assertFalse(Database.pop("test-event", event))
    }

    @Test
    fun `events that are not borrowed are not found`() {
        val id = submit("hello")

        listOf(id, Uuid.random()).forEach {
            mockMvc.post("/api/complete-event/$it").andExpect { status { isNotFound() } }
        }
        assertEquals(id, Database.borrow("test-event")?.id)
    }

    @Test
    fun `invalid event ids are bad requests`() {
        listOf("abc", "123", "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", "${Uuid.random()}0").forEach {
            mockMvc.post("/api/complete-event/$it").andExpect { status { isBadRequest() } }
        }
    }

    @Test
    fun `submitting an unknown event type is not found`() {
        mockMvc.post("/api/submit-event/unknown") {
            contentType = MediaType.TEXT_PLAIN
            content = "hello"
        }.andExpect { status { isNotFound() } }

        assertNull(Database.borrow("test-event"))
    }
}
