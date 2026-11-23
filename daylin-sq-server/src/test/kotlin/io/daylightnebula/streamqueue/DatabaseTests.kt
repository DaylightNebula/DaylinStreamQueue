package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMode
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.DBOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteOptions
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.uuid.Uuid

/** Uses a temporary folder for every test, so the real data folder is never touched. */
class DatabaseTests {

    @TempDir
    lateinit var folder: Path

    private lateinit var db: RocksDB
    private lateinit var first: Database.EventStore
    private lateinit var second: Database.EventStore

    private val notExpired = Config.Durations(20.minutes, 20.minutes, 24.hours)
    private val expired = notExpired.copy(maxNoReceiveDuration = 1.milliseconds, maxProcessingDuration = 1.milliseconds)
    private val receiveExpired = notExpired.copy(maxNoReceiveDuration = 1.milliseconds)
    private val processingExpired = notExpired.copy(maxProcessingDuration = 1.milliseconds)
    private val tooOld = notExpired.copy(maxTimeInQueue = 1.milliseconds)

    @BeforeTest
    fun open() {
        RocksDB.loadLibrary()
        Database.writeOptions = WriteOptions()
        val handles = mutableListOf<ColumnFamilyHandle>()
        val families = listOf("default", "first", "second").map { ColumnFamilyDescriptor(it.encodeToByteArray()) }
        val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, folder.toString(), families, handles)
        first = Database.EventStore(db, handles[1], EventMode.Simple)
        second = Database.EventStore(db, handles[2], EventMode.Simple)
    }

    @AfterTest
    fun close() {
        db.close()
        Database.stores = emptyMap()
        Config.config = null
    }

    private fun reopen() {
        db.close()
        open()
    }

    private fun Database.EventStore.pushOne(message: String = "message"): Uuid {
        val id = Uuid.random()
        push(listOf(id to message))
        return id
    }

    private fun waitUntilExpired() = Thread.sleep(10)

    @Test
    fun `borrow returns null when the queue is empty`() {
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `borrow returns the event that was pushed`() {
        val before = Clock.System.now() - 1.milliseconds
        val id = first.pushOne("hello")

        val event = assertNotNull(first.borrow(notExpired))
        assertEquals(id, event.id)
        assertEquals("hello", event.message)
        assertTrue(event.recvTime in before..Clock.System.now())
    }

    @Test
    fun `messages keep empty and non ascii text`() {
        val messages = listOf("", "héllo", "日本", "😀", "line\nbreak")
        messages.forEach { first.pushOne(it) }

        assertEquals(messages, List(messages.size) { first.borrow(notExpired)?.message })
    }

    @Test
    fun `an event has no contact address unless it is pushed with one`() {
        first.pushOne()
        first.push(listOf(Uuid.random() to "hello"), "http://producer/contact")

        assertNull(assertNotNull(first.borrow(notExpired)).contact)
        val event = assertNotNull(first.borrow(notExpired))
        assertEquals("http://producer/contact", event.contact)
        assertEquals("hello", event.message)
    }

    @Test
    fun `contact addresses and messages keep empty and non ascii text`() {
        val messages = listOf("", "héllo", "日本", "😀")
        messages.forEach { first.push(listOf(Uuid.random() to it), "http://producer/日本?a=é") }

        repeat(messages.size) {
            val event = assertNotNull(first.borrow(notExpired))
            assertEquals(messages[it], event.message)
            assertEquals("http://producer/日本?a=é", event.contact)
        }
    }

    @Test
    fun `a contact address is kept when an event is given back, received, expired or reloaded`() {
        first.push(listOf(Uuid.random() to "hello"), "http://producer/contact")
        fun check(event: Database.BorrowedEvent?) {
            assertEquals("http://producer/contact", assertNotNull(event).contact)
            assertEquals("hello", event.message)
        }

        reopen()
        val event = assertNotNull(first.borrow(notExpired))
        check(event)
        first.giveBack(event)
        val again = assertNotNull(first.borrow(notExpired))
        check(again)
        first.markReceived(again)
        reopen()
        waitUntilExpired()
        check(first.borrow(processingExpired))
    }

    @Test
    fun `events are borrowed in the order they were pushed`() {
        val ids = List(300) { first.pushOne() }

        assertEquals<List<Uuid?>>(ids, List(ids.size) { first.borrow(notExpired)?.id })
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `a batch keeps its order and follows earlier events`() {
        val single = first.pushOne()
        val batch = List(3) { Uuid.random() to "message $it" }
        first.push(batch)

        assertEquals<List<Uuid?>>(listOf(single) + batch.map { it.first }, List(4) { first.borrow(notExpired)?.id })
    }

    @Test
    fun `an event pushed after a batch does not overwrite it`() {
        val batch = List(3) { Uuid.random() to "message $it" }
        first.push(batch)
        val last = first.pushOne()

        assertEquals<List<Uuid?>>(batch.map { it.first } + last, List(4) { first.borrow(notExpired)?.id })
    }

    @Test
    fun `an empty batch adds nothing`() {
        first.push(emptyList())

        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `queues are independent`() {
        val id = first.pushOne()

        assertNull(second.borrow(notExpired))
        assertEquals(id, first.borrow(notExpired)?.id)
    }

    @Test
    fun `a borrowed event is hidden until it expires`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()

        assertNull(first.borrow(notExpired))
        assertEquals(id, first.borrow(expired)?.id)
    }

    @Test
    fun `expired events are borrowed before new ones`() {
        val old = first.pushOne()
        val new = first.pushOne()
        assertEquals(old, first.borrow(notExpired)?.id)
        waitUntilExpired()

        assertEquals(old, first.borrow(expired)?.id)
        assertEquals(new, first.borrow(notExpired)?.id)
    }

    @Test
    fun `borrowing an expired event hides it again`() {
        first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()
        assertNotNull(first.borrow(expired))

        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `pop removes the event for good`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))

        assertTrue(first.pop(event))
        waitUntilExpired()
        assertNull(first.borrow(expired))
    }

    @Test
    fun `pop only works once`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))

        assertTrue(first.pop(event))
        assertFalse(first.pop(event))
    }

    @Test
    fun `pop removes the borrowed event and not the oldest one`() {
        val oldest = first.pushOne()
        val newest = first.pushOne()
        first.borrow(notExpired)
        assertTrue(first.pop(assertNotNull(first.borrow(notExpired))))
        waitUntilExpired()

        assertEquals(oldest, first.borrow(expired)?.id)
        assertNull(first.borrow(notExpired), "$newest should have been removed")
    }

    @Test
    fun `pop is rejected after someone else borrows the expired event`() {
        first.pushOne()
        val stale = assertNotNull(first.borrow(notExpired))
        waitUntilExpired()
        val current = assertNotNull(first.borrow(expired))

        assertFalse(first.pop(stale))
        assertTrue(first.pop(current))
    }

    @Test
    fun `pop still works after expiry when nobody else borrowed the event`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        waitUntilExpired()

        assertTrue(first.pop(event))
    }

    @Test
    fun `waiting, borrowed and popped events survive a restart`() {
        val ids = List(4) { first.pushOne() }
        val popped = assertNotNull(first.borrow(notExpired))
        val borrowed = assertNotNull(first.borrow(notExpired))
        first.pop(popped)
        reopen()

        assertEquals(ids[2], first.borrow(notExpired)?.id)
        assertEquals(ids[3], first.borrow(notExpired)?.id)
        assertNull(first.borrow(notExpired))
        assertTrue(first.pop(borrowed))
    }

    @Test
    fun `a popped event does not come back after a restart`() {
        first.pushOne()
        assertTrue(first.pop(assertNotNull(first.borrow(notExpired))))
        reopen()
        waitUntilExpired()

        assertNull(first.borrow(expired))
    }

    @Test
    fun `a borrowed event still expires after a restart`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        reopen()
        waitUntilExpired()

        assertEquals(id, first.borrow(expired)?.id)
    }

    @Test
    fun `events pushed after a restart do not overwrite older ones`() {
        val borrowed = first.pushOne()
        val waiting = first.pushOne()
        first.borrow(notExpired)
        reopen()
        val new = first.pushOne()

        assertEquals(waiting, first.borrow(notExpired)?.id)
        assertEquals(new, first.borrow(notExpired)?.id)
        waitUntilExpired()
        assertEquals(borrowed, first.borrow(expired)?.id)
    }

    @Test
    fun `events pushed after a restart do not overwrite borrowed ones when nothing is waiting`() {
        val borrowed = first.pushOne()
        first.borrow(notExpired)
        reopen()
        val new = first.pushOne()

        assertEquals(new, first.borrow(notExpired)?.id)
        waitUntilExpired()
        assertEquals(setOf<Uuid?>(borrowed, new), List(2) { first.borrow(expired.copy(maxNoReceiveDuration = 5.milliseconds))?.id }.toSet())
    }

    @Test
    fun `an unreceived event only uses the no receive duration`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()

        assertNull(first.borrow(processingExpired))
        assertEquals(id, first.borrow(receiveExpired)?.id)
    }

    @Test
    fun `a received event only uses the processing duration`() {
        val id = first.pushOne("hello")
        assertTrue(first.markReceived(assertNotNull(first.borrow(notExpired))))
        waitUntilExpired()

        assertNull(first.borrow(receiveExpired))
        val again = assertNotNull(first.borrow(processingExpired))
        assertEquals(id, again.id)
        assertEquals("hello", again.message)
    }

    @Test
    fun `processing time starts when the event is received`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        Thread.sleep(600)
        first.markReceived(event)

        assertNull(first.borrow(notExpired.copy(maxProcessingDuration = 500.milliseconds)))
    }

    @Test
    fun `marking an event as received again does not extend its time`() {
        val id = first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        assertTrue(first.markReceived(event))
        Thread.sleep(600)

        assertTrue(first.markReceived(event))
        assertEquals(id, first.borrow(notExpired.copy(maxProcessingDuration = 500.milliseconds))?.id)
    }

    @Test
    fun `an event borrowed again after being received must be received again`() {
        val id = first.pushOne()
        first.markReceived(assertNotNull(first.borrow(notExpired)))
        waitUntilExpired()
        assertNotNull(first.borrow(processingExpired))
        waitUntilExpired()

        assertNull(first.borrow(processingExpired))
        assertEquals(id, first.borrow(receiveExpired)?.id)
    }

    @Test
    fun `a received event can be popped with the borrowed event`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        first.markReceived(event)

        assertTrue(first.pop(event))
        assertFalse(first.pop(event))
        assertFalse(first.markReceived(event))
        reopen()
        waitUntilExpired()
        assertNull(first.borrow(expired))
    }

    @Test
    fun `a stale consumer cannot mark or pop an event that was borrowed again`() {
        first.pushOne()
        val stale = assertNotNull(first.borrow(notExpired))
        waitUntilExpired()
        val current = assertNotNull(first.borrow(expired))

        assertFalse(first.markReceived(stale))
        assertTrue(first.markReceived(current))
        assertFalse(first.pop(stale))
        assertTrue(first.pop(current))
    }

    @Test
    fun `received events stay received after a restart`() {
        val id = first.pushOne("hello")
        val event = assertNotNull(first.borrow(notExpired))
        first.markReceived(event)
        reopen()
        waitUntilExpired()

        assertNull(first.borrow(receiveExpired))
        val again = assertNotNull(first.borrow(processingExpired))
        assertEquals(id, again.id)
        assertEquals("hello", again.message)
        assertEquals(event.recvTime, again.recvTime)
        assertFalse(first.pop(event))
        assertTrue(first.pop(again))
    }

    @Test
    fun `a received event can be popped after a restart`() {
        first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        first.markReceived(event)
        reopen()

        assertTrue(first.pop(event))
    }

    @Test
    fun `events pushed after a restart do not overwrite received ones`() {
        val received = first.pushOne()
        first.markReceived(assertNotNull(first.borrow(notExpired)))
        reopen()
        val new = first.pushOne()

        assertEquals(new, first.borrow(notExpired)?.id)
        waitUntilExpired()
        assertEquals(received, first.borrow(processingExpired)?.id)
    }

    @Test
    fun `borrow skips and removes waiting events that are too old`() {
        repeat(3) { first.pushOne() }
        Thread.sleep(600)
        val new = first.pushOne()

        assertEquals(new, first.borrow(notExpired.copy(maxTimeInQueue = 500.milliseconds))?.id)
        assertNull(first.borrow(notExpired))
        reopen()
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `borrow does not hand out an expired event that is too old`() {
        first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()

        assertNull(first.borrow(expired.copy(maxTimeInQueue = 1.milliseconds)))
        reopen()
        assertNull(first.borrow(expired))
    }

    @Test
    fun `removeOld removes every waiting event that is too old`() {
        first.push(List(2_500) { Uuid.random() to "message" })
        waitUntilExpired()
        first.removeOld(tooOld)

        assertNull(first.borrow(notExpired))
        reopen()
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `removeOld keeps events that are not too old`() {
        val old = first.pushOne()
        Thread.sleep(600)
        val new = first.pushOne()
        first.removeOld(notExpired.copy(maxTimeInQueue = 500.milliseconds))

        assertEquals(new, first.borrow(notExpired)?.id)
        assertNull(first.borrow(notExpired), "$old should have been removed")
    }

    @Test
    fun `removeOld keeps events that a consumer may still hold`() {
        repeat(2) { first.pushOne() }
        val received = assertNotNull(first.borrow(notExpired))
        val unreceived = assertNotNull(first.borrow(notExpired))
        first.markReceived(received)
        waitUntilExpired()
        first.removeOld(tooOld)

        assertTrue(first.pop(received))
        assertTrue(first.pop(unreceived))
    }

    @Test
    fun `removeOld removes held events that are expired and too old`() {
        repeat(2) { first.pushOne() }
        val received = assertNotNull(first.borrow(notExpired))
        val unreceived = assertNotNull(first.borrow(notExpired))
        first.markReceived(received)
        waitUntilExpired()
        first.removeOld(expired.copy(maxTimeInQueue = 1.milliseconds))

        assertFalse(first.pop(received))
        assertFalse(first.pop(unreceived))
        reopen()
        assertNull(first.borrow(expired))
    }

    @Test
    fun `removeOld does not touch expired events that are not too old`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()
        first.removeOld(expired)

        assertEquals(id, first.borrow(expired)?.id)
    }

    @Test
    fun `a received event can be popped by id`() {
        first.pushOne()
        val id = first.pushOne()
        val other = assertNotNull(first.borrow(notExpired))
        val event = assertNotNull(first.borrow(notExpired))
        assertEquals(id, event.id)

        assertTrue(first.markReceived(event))
        waitUntilExpired()
        assertEquals(other.id, first.borrow(receiveExpired)?.id)
        assertNull(first.borrow(notExpired.copy(maxNoReceiveDuration = 1.hours)))
        assertTrue(first.pop(id))
        assertFalse(first.pop(id))
        assertFalse(first.markReceived(event))
        assertFalse(first.pop(event))
    }

    @Test
    fun `an event that is not borrowed cannot be popped by id`() {
        val id = first.pushOne()

        assertFalse(first.pop(id))
        assertFalse(first.pop(Uuid.random()))
        assertEquals(id, first.borrow(notExpired)?.id)
    }

    @Test
    fun `borrowed and received events can be found by id after a restart`() {
        val ids = List(2) { first.pushOne() }
        val events = List(2) { assertNotNull(first.borrow(notExpired)) }
        first.markReceived(events[0])
        reopen()

        assertTrue(first.pop(ids[0]))
        assertTrue(first.markReceived(events[1]))
        assertTrue(first.pop(ids[1]))
    }

    @Test
    fun `an event borrowed again can still be found by id`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()
        val current = assertNotNull(first.borrow(expired))

        assertTrue(first.pop(id))
        assertFalse(first.pop(current))
    }

    @Test
    fun `removed events cannot be found by id`() {
        val id = first.pushOne()
        first.borrow(notExpired)
        waitUntilExpired()
        first.removeOld(expired.copy(maxTimeInQueue = 1.milliseconds))

        assertFalse(first.pop(id))
    }

    @Test
    fun `database finds events by id in every store`() {
        Database.stores = mapOf("first" to first, "second" to second)
        val ids = listOf(first.pushOne(), second.pushOne())
        first.borrow(notExpired)
        second.borrow(notExpired)

        ids.forEach {
            assertTrue(Database.pop(it))
            assertFalse(Database.pop(it))
        }
        assertFalse(Database.pop(Uuid.random()))
    }

    @Test
    fun `an event that is given back is borrowed before newer ones`() {
        val ids = List(3) { first.pushOne("message $it") }
        val borrowed = assertNotNull(first.borrow(notExpired))
        assertTrue(first.giveBack(borrowed))

        val again = List(3) { assertNotNull(first.borrow(notExpired)) }
        assertEquals(ids, again.map { it.id })
        assertEquals("message 0", again[0].message)
        assertEquals(borrowed.recvTime, again[0].recvTime)
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `events given back in any order keep the order they were pushed in`() {
        val ids = List(3) { first.pushOne() }
        val borrowed = List(3) { assertNotNull(first.borrow(notExpired)) }
        assertTrue(first.giveBack(borrowed[0]))
        assertTrue(first.giveBack(borrowed[2]))
        assertTrue(first.giveBack(borrowed[1]))

        assertEquals<List<Uuid?>>(ids, List(3) { first.borrow(notExpired)?.id })
    }

    @Test
    fun `a received event can be given back and keeps its message`() {
        val id = first.pushOne("hello")
        val event = assertNotNull(first.borrow(notExpired))
        first.markReceived(event)
        assertTrue(first.giveBack(event))

        val again = assertNotNull(first.borrow(notExpired))
        assertEquals(id, again.id)
        assertEquals("hello", again.message)
    }

    @Test
    fun `an event that is given back is no longer held`() {
        val id = first.pushOne()
        val event = assertNotNull(first.borrow(notExpired))
        assertTrue(first.giveBack(event))

        assertFalse(first.giveBack(event))
        assertFalse(first.pop(event))
        assertFalse(first.pop(id))
        assertFalse(first.markReceived(event))
    }

    @Test
    fun `a removed or stale event cannot be given back`() {
        first.pushOne()
        val stale = assertNotNull(first.borrow(notExpired))
        waitUntilExpired()
        val current = assertNotNull(first.borrow(expired))

        assertFalse(first.giveBack(stale))
        assertTrue(first.pop(current))
        assertFalse(first.giveBack(current))
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `an event that was given back survives a restart`() {
        val ids = List(2) { first.pushOne() }
        first.giveBack(assertNotNull(first.borrow(notExpired)))
        reopen()

        assertEquals<List<Uuid?>>(ids, List(2) { first.borrow(notExpired)?.id })
    }

    @Test
    fun `an event that was given back is still removed when it is too old`() {
        first.pushOne()
        first.giveBack(assertNotNull(first.borrow(notExpired)))
        waitUntilExpired()
        first.removeOld(tooOld)

        assertFalse(first.hasAvailable(notExpired))
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `hasAvailable is true for waiting and expired events`() {
        assertFalse(first.hasAvailable(notExpired))
        first.pushOne()
        assertTrue(first.hasAvailable(notExpired))

        val event = assertNotNull(first.borrow(notExpired))
        waitUntilExpired()
        assertFalse(first.hasAvailable(notExpired))
        assertTrue(first.hasAvailable(expired))

        first.pop(event)
        assertFalse(first.hasAvailable(expired))
    }

    @Test
    fun `database complete returns the type of the event`() {
        Database.stores = mapOf("first" to first, "second" to second)
        val id = second.pushOne()
        assertNull(Database.complete(id))
        second.borrow(notExpired)

        assertEquals("second", Database.complete(id))
        assertNull(Database.complete(id))
    }

    @Test
    @Timeout(60)
    fun `concurrent producers and consumers handle every event exactly once`() {
        val producers = 4
        val perProducer = 2_500
        val total = producers * perProducer
        val pushed = ConcurrentLinkedQueue<Uuid>()
        val handled = ConcurrentLinkedQueue<Uuid>()
        val count = AtomicInteger()

        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            val producing = List(producers) {
                executor.submit(Callable { repeat(perProducer) { pushed += first.pushOne() } })
            }
            val consuming = List(4) {
                executor.submit(Callable {
                    while (count.get() < total && !Thread.currentThread().isInterrupted) {
                        val event = first.borrow(notExpired)
                        if (event == null) {
                            Thread.sleep(1)
                            continue
                        }
                        check(first.markReceived(event))
                        check(first.pop(event))
                        handled += event.id
                        count.incrementAndGet()
                    }
                })
            }
            (producing + consuming).forEach { it.get() }
        }

        assertEquals(total, handled.size)
        assertEquals(pushed.toSet(), handled.toSet())
        assertNull(first.borrow(notExpired))
    }

    @Test
    fun `database functions use the store with the given name`() {
        Config.config = Config(durations = notExpired)
        Database.stores = mapOf("first" to first, "second" to second)
        val id = Uuid.random()
        val batch = List(2) { Uuid.random() to "message" }
        Database.push("first", id, "hello")
        Database.pushAll("second", batch)

        assertEquals<List<Uuid?>>(batch.map { it.first }, List(2) { Database.borrow("second")?.id })
        val event = assertNotNull(Database.borrow("first"))
        assertEquals(id, event.id)
        assertTrue(Database.markReceived("first", event))
        assertTrue(Database.pop("first", event))
        assertFalse(Database.pop("first", event))
        assertFalse(Database.markReceived("first", event))
    }

    @Test
    fun `database borrow uses the durations from the config`() {
        Database.stores = mapOf("first" to first)
        val id = first.pushOne()
        Config.config = Config(durations = notExpired)
        assertEquals(id, Database.borrow("first")?.id)
        waitUntilExpired()
        assertNull(Database.borrow("first"))

        Config.config = Config(durations = expired)
        assertEquals(id, Database.borrow("first")?.id)
    }

    @Test
    fun `database functions reject unknown events`() {
        Config.config = Config(durations = notExpired)
        Database.stores = mapOf("first" to first)

        assertFailsWith<NoSuchElementException> { Database.push("unknown", Uuid.random(), "message") }
        assertFailsWith<NoSuchElementException> { Database.borrow("unknown") }
    }

    private fun openDatabase() {
        db.close()
        Database.open(folder.toString())
        db = Database.db
    }

    @Test
    fun `nothing is registered in a new database`() {
        openDatabase()

        assertTrue(Database.stores.isEmpty())
    }

    @Test
    fun `a registered event type can be used and survives a restart`() {
        Config.config = Config(durations = notExpired)
        openDatabase()
        Database.register("simple", EventMode.Simple)
        Database.register("response", EventMode.Response)
        val id = Uuid.random()
        Database.push("simple", id, "hello")
        openDatabase()

        val modes = Database.stores.mapValues { it.value.mode }
        assertEquals(mapOf("simple" to EventMode.Simple, "response" to EventMode.Response), modes)
        assertEquals(id, Database.borrow("simple")?.id)
    }

    @Test
    fun `an event type can only be registered again with the same mode`() {
        openDatabase()
        Database.register("simple", EventMode.Simple)
        val store = Database.stores.getValue("simple")
        Database.register("simple", EventMode.Simple)

        assertFailsWith<ModeConflictException> { Database.register("simple", EventMode.Response) }
        assertTrue(store === Database.stores.getValue("simple"))
        openDatabase()
        assertEquals(EventMode.Simple, Database.stores.getValue("simple").mode)
    }

    @Test
    fun `events from before event types were registered are kept`() {
        Config.config = Config(durations = notExpired)
        val id = first.pushOne()
        openDatabase()
        assertTrue(Database.stores.isEmpty())

        Database.register("first", EventMode.Response)

        assertEquals(id, Database.borrow("first")?.id)
    }

    @Test
    fun `names used by the database cannot be registered`() {
        openDatabase()
        listOf("daylin-sq-subscriptions", "daylin-sq-event-types").forEach {
            assertTrue(Database.isReserved(it))
            assertFailsWith<IllegalArgumentException> { Database.register(it, EventMode.Simple) }
        }
        assertFalse(Database.isReserved("first"))
        assertTrue(Database.stores.isEmpty())
    }

    @Test
    fun `registering at once makes one store`() {
        openDatabase()
        Executors.newFixedThreadPool(8).use { pool ->
            val tasks = List(8) { Callable { Database.register("simple", EventMode.Simple) } }
            pool.invokeAll(tasks).forEach { it.get() }
        }

        assertEquals(setOf("simple"), Database.stores.keys)
    }
}
