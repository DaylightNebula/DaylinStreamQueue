package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMode
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.DBOptions
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.TreeSet
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlin.uuid.getUuid
import kotlin.uuid.putUuid

/**
 * Thrown when an event type is already registered with another mode.
 *
 * @param eventType the event type
 * @param mode the mode that it is registered with
 */
class ModeConflictException(eventType: String, mode: EventMode):
    IllegalStateException("The event type $eventType is already registered with the mode $mode")

object Database {

    private const val PATH = "data"
    private const val SUBSCRIPTIONS_FAMILY = "daylin-sq-subscriptions"
    private const val EVENT_TYPES_FAMILY = "daylin-sq-event-types"
    private const val READY: Byte = 0
    private const val TAKEN: Byte = 1
    private const val RECEIVED: Byte = 2
    private const val ID_OFFSET = 8
    private const val RECV_TIME_OFFSET = 24
    private const val CONTACT_LENGTH_OFFSET = 32
    private const val CONTACT_OFFSET = 36

    private const val CLEANUP_BATCH = 1_000
    private val cleanupInterval = 1.minutes
    private val logger = LoggerFactory.getLogger(Database::class.java)

    lateinit var writeOptions: WriteOptions
    lateinit var db: RocksDB

    @Volatile
    lateinit var stores: Map<String, EventStore>

    // every column family by name, including ones of event types that are not registered
    lateinit var families: Map<String, ColumnFamilyHandle>
        private set
    private val registerLock = ReentrantLock()

    /**
     * An event that is hidden from other borrows.
     *
     * @param seq the position of the event in its queue
     * @param takenAt when the event was borrowed, in epoch milliseconds
     * @param id the id of the event
     * @param recvTime when the event was pushed
     * @param message the message of the event
     * @param contact where the producer of the event may be contacted, or null if it did not provide an address
     */
    data class BorrowedEvent(
        val seq: Long,
        val takenAt: Long,
        val id: Uuid,
        val recvTime: Instant,
        val message: String,
        val contact: String? = null
    )

    /**
     * One queue, kept in its own column family. Values are takenAt, id, recvTime, the length of the contact address (0
     * if there is none), the contact address, then the message. Received events have receivedAt in front of that.
     *
     * @param db the database to keep the queue in
     * @param family the column family of the queue
     * @param mode how the events in the queue are handled
     */
    class EventStore(private val db: RocksDB, private val family: ColumnFamilyHandle, val mode: EventMode) {
        private data class Held(
            val seq: Long,
            val takenAt: Long,
            val id: Uuid,
            val recvTime: Long,
            val receivedAt: Long? = null
        ) {
            // when the current wait started
            val since = receivedAt ?: takenAt
        }

        // pushes also take the lock, otherwise a slow push could commit behind head and never be read
        private val lock = ReentrantLock()
        private val held = HashMap<Long, Held>()
        private val heldById = HashMap<Uuid, Held>()
        private val unreceived = TreeSet<Held>(compareBy({ it.since }, { it.seq }))
        private val received = TreeSet<Held>(compareBy({ it.since }, { it.seq }))
        private var head = 0L
        private var tail = 0L

        init {
            db.newIterator(family).use { iter ->
                iter.seek(key(TAKEN, 0))
                while (iter.isValid) {
                    val seq = seqOf(iter.key())
                    val buffer = ByteBuffer.wrap(iter.value())
                    val receivedAt = if (iter.key()[0] == RECEIVED) buffer.getLong() else null
                    val value = buffer.slice()
                    hold(
                        Held(
                            seq = seq,
                            takenAt = value.getLong(0),
                            id = value.getUuid(ID_OFFSET),
                            recvTime = value.getLong(RECV_TIME_OFFSET),
                            receivedAt = receivedAt
                        )
                    )
                    tail = maxOf(tail, seq + 1)
                    iter.next()
                }
                iter.status()

                iter.seekForPrev(key(READY, Long.MAX_VALUE))
                if (iter.isValid) tail = maxOf(tail, seqOf(iter.key()) + 1)
                iter.status()
            }
        }

        /**
         * Adds events to the back of the queue in one write.
         *
         * @param messages the id and message of each event
         * @param contact where the producer of the events may be contacted, if it provided an address
         */
        fun push(messages: List<Pair<Uuid, String>>, contact: String? = null) {
            lock.withLock {
                val now = Clock.System.now().toEpochMilliseconds()
                WriteBatch().use { batch ->
                    messages.forEachIndexed { i, (id, message) ->
                        batch.put(family, key(READY, tail + i), encode(id, now, message, contact))
                    }
                    db.write(writeOptions, batch)
                }
                tail += messages.size
            }
        }

        /**
         * Checks if there is an event to borrow. [borrow] may still return null if every event is too old.
         *
         * @param durations how long events may be held for
         * @return true if an event is waiting, or is held by a consumer that has run out of time
         */
        fun hasAvailable(durations: Config.Durations): Boolean = lock.withLock {
            val now = Clock.System.now().toEpochMilliseconds()
            expired(now, durations).any() || nextReady() != null
        }

        /**
         * Takes the oldest available event and hides it from other borrows. Events that are too old are removed.
         *
         * @param durations how long events may be held for and stay in the queue
         * @return the borrowed event, or null if there is none
         */
        fun borrow(durations: Config.Durations): BorrowedEvent? {
            lock.withLock {
                val now = Clock.System.now().toEpochMilliseconds()
                while (true) {
                    val expired = expired(now, durations).firstOrNull()
                    val (seq, value) = if (expired != null) expired.seq to read(expired) else nextReady() ?: return null
                    val oldKey = if (expired != null) keyOf(expired) else key(READY, seq)
                    val recvTime = ByteBuffer.wrap(value).getLong(RECV_TIME_OFFSET)
                    val tooOld = recvTime.olderThan(durations.maxTimeInQueue, now)

                    ByteBuffer.wrap(value).putLong(now)
                    WriteBatch().use { batch ->
                        batch.delete(family, oldKey)
                        if (!tooOld) batch.put(family, key(TAKEN, seq), value)
                        db.write(writeOptions, batch)
                    }

                    if (expired != null) release(expired) else head = seq + 1
                    if (tooOld) continue
                    val event = decode(seq, value)
                    hold(Held(seq, now, event.id, recvTime))
                    return event
                }
            }
        }

        /**
         * Puts a borrowed event back at the front of the queue.
         *
         * @param borrowed the event to put back
         * @return false if it was removed or has since been borrowed again
         */
        fun giveBack(borrowed: BorrowedEvent): Boolean {
            lock.withLock {
                val current = find(borrowed) ?: return false
                val value = read(current)
                WriteBatch().use { batch ->
                    batch.delete(family, keyOf(current))
                    batch.put(family, key(READY, current.seq), value)
                    db.write(writeOptions, batch)
                }
                release(current)

                // everything before head is already gone, so keeping its seq puts it at the front
                head = minOf(head, current.seq)
                return true
            }
        }

        /**
         * Marks a borrowed event as received, so that it is held for the processing duration.
         *
         * @param borrowed the event to mark
         * @return false if it was removed or has since been borrowed again
         */
        fun markReceived(borrowed: BorrowedEvent): Boolean = lock.withLock { markReceived(find(borrowed)) }

        private fun markReceived(current: Held?): Boolean {
            if (current == null) return false
            if (current.receivedAt != null) return true

            val now = Clock.System.now().toEpochMilliseconds()
            val next = current.copy(receivedAt = now)
            val value = read(current)
            WriteBatch().use { batch ->
                batch.delete(family, keyOf(current))
                batch.put(family, keyOf(next), ByteBuffer.allocate(8 + value.size).putLong(now).put(value).array())
                db.write(writeOptions, batch)
            }
            release(current)
            hold(next)
            return true
        }

        /**
         * Removes a finished event.
         *
         * @param borrowed the event to remove
         * @return false if it was already removed or has since been borrowed again
         */
        fun pop(borrowed: BorrowedEvent): Boolean = lock.withLock { pop(find(borrowed)) }

        /**
         * Removes a finished event, but cannot tell if it was borrowed again.
         *
         * @param id the id of the event to remove
         * @return false if it is not borrowed
         */
        fun pop(id: Uuid): Boolean = lock.withLock { pop(heldById[id]) }

        private fun pop(current: Held?): Boolean {
            if (current == null) return false
            db.delete(family, writeOptions, keyOf(current))
            release(current)
            return true
        }

        /**
         * Removes events past [Config.Durations.maxTimeInQueue], except ones a consumer is still allowed to hold.
         *
         * @param durations how long events may be held for and stay in the queue
         */
        fun removeOld(durations: Config.Durations) {
            // in batches, so a large cleanup does not block the queue
            do {
                val removed = lock.withLock { removeOldBatch(durations) }
            } while (removed == CLEANUP_BATCH)
        }

        private fun removeOldBatch(durations: Config.Durations): Int {
            val now = Clock.System.now().toEpochMilliseconds()
            val oldHeld = expired(now, durations)
                .filter { it.recvTime.olderThan(durations.maxTimeInQueue, now) }
                .take(CLEANUP_BATCH)
                .toList()
            val oldReady = mutableListOf<Long>()
            db.newIterator(family).use { iter ->
                iter.seek(key(READY, head))
                while (oldHeld.size + oldReady.size < CLEANUP_BATCH && iter.isValid && iter.key()[0] == READY) {
                    val recvTime = ByteBuffer.wrap(iter.value()).getLong(RECV_TIME_OFFSET)
                    if (!recvTime.olderThan(durations.maxTimeInQueue, now)) break
                    oldReady += seqOf(iter.key())
                    iter.next()
                }
                iter.status()
            }

            WriteBatch().use { batch ->
                oldHeld.forEach { batch.delete(family, keyOf(it)) }
                oldReady.forEach { batch.delete(family, key(READY, it)) }
                db.write(writeOptions, batch)
            }
            oldHeld.forEach { release(it) }
            oldReady.lastOrNull()?.let { head = it + 1 }
            return oldHeld.size + oldReady.size
        }

        /** Held events whose consumer has run out of time. */
        private fun expired(now: Long, durations: Config.Durations): Sequence<Held> =
            unreceived.asSequence().takeWhile { it.since.olderThan(durations.maxNoReceiveDuration, now) } +
                received.asSequence().takeWhile { it.since.olderThan(durations.maxProcessingDuration, now) }

        private fun Long.olderThan(duration: Duration, now: Long) = this <= now - duration.inWholeMilliseconds

        private fun find(borrowed: BorrowedEvent) = held[borrowed.seq]?.takeIf { it.takenAt == borrowed.takenAt }

        private fun groupOf(entry: Held) = if (entry.receivedAt == null) unreceived else received

        private fun hold(entry: Held) {
            held[entry.seq] = entry
            heldById[entry.id] = entry
            groupOf(entry) += entry
        }

        private fun release(entry: Held) {
            held -= entry.seq
            heldById.remove(entry.id, entry)
            groupOf(entry) -= entry
        }

        private fun keyOf(entry: Held) = key(if (entry.receivedAt == null) TAKEN else RECEIVED, entry.seq)

        private fun read(entry: Held): ByteArray {
            val value = db.get(family, keyOf(entry))
            return if (entry.receivedAt == null) value else value.copyOfRange(8, value.size)
        }

        private fun nextReady(): Pair<Long, ByteArray>? = db.newIterator(family).use { iter ->
            // start at head to skip over events that are already gone
            iter.seek(key(READY, head))
            iter.status()
            if (iter.isValid && iter.key()[0] == READY) seqOf(iter.key()) to iter.value() else null
        }

        private fun key(prefix: Byte, seq: Long) = ByteBuffer.allocate(9).put(prefix).putLong(seq).array()

        private fun seqOf(key: ByteArray) = ByteBuffer.wrap(key).getLong(1)

        private fun encode(id: Uuid, recvTime: Long, message: String, contact: String?): ByteArray {
            val bytes = message.encodeToByteArray()
            val contactBytes = contact.orEmpty().encodeToByteArray()
            return ByteBuffer.allocate(CONTACT_OFFSET + contactBytes.size + bytes.size)
                .putLong(0)
                .putUuid(id)
                .putLong(recvTime)
                .putInt(contactBytes.size)
                .put(contactBytes)
                .put(bytes)
                .array()
        }

        private fun decode(seq: Long, value: ByteArray): BorrowedEvent {
            val buffer = ByteBuffer.wrap(value)
            val messageOffset = CONTACT_OFFSET + buffer.getInt(CONTACT_LENGTH_OFFSET)
            return BorrowedEvent(
                seq = seq,
                takenAt = buffer.getLong(),
                id = buffer.getUuid(),
                recvTime = Instant.fromEpochMilliseconds(buffer.getLong()),
                message = value.decodeToString(startIndex = messageOffset),
                contact = value.decodeToString(CONTACT_OFFSET, messageOffset).ifEmpty { null }
            )
        }
    }

    /** Opens the database and starts removing old events. */
    fun init() {
        open(PATH)
        thread(isDaemon = true, name = "event-cleanup") {
            while (true) {
                Thread.sleep(cleanupInterval.inWholeMilliseconds)
                stores.forEach { (event, store) ->
                    runCatching { store.removeOld(Config.get().durations) }
                        .onFailure { logger.error("Failed to remove old events from {}", event, it) }
                }
            }
        }
    }

    /**
     * Opens the database with a store for each event type that is registered and loads the subscriptions.
     *
     * @param path the folder of the database
     */
    fun open(path: String) {
        // setup Rock DB
        RocksDB.loadLibrary()
        writeOptions = WriteOptions().setSync(false)

        // RocksDB must open every existing column family, including ones that are not registered
        val existing = Options().use { RocksDB.listColumnFamilies(it, path) }.map { it.decodeToString() }
        val names = (existing + "default" + SUBSCRIPTIONS_FAMILY + EVENT_TYPES_FAMILY).distinct()

        val handles = mutableListOf<ColumnFamilyHandle>()
        val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, path, names.map { ColumnFamilyDescriptor(it.encodeToByteArray()) }, handles)
        families = names.zip(handles).toMap()
        stores = loadEventTypes().mapValues { (eventType, mode) ->
            EventStore(db, families.getValue(eventType), mode)
        }
        Subscriptions.init(SubscriptionStore(db, families.getValue(SUBSCRIPTIONS_FAMILY)))
    }

    /**
     * Checks if a name is used by the database, and so cannot be an event type.
     *
     * @param eventType the name to check
     * @return true if it is used by the database
     */
    fun isReserved(eventType: String): Boolean = eventType == SUBSCRIPTIONS_FAMILY || eventType == EVENT_TYPES_FAMILY

    /**
     * Registers an event type so that its events can be submitted, does nothing if it is already registered.
     *
     * @param eventType the event type, cannot be reserved
     * @param mode how its events are handled
     * @throws ModeConflictException if the event type is already registered with another mode
     */
    fun register(eventType: String, mode: EventMode) {
        registerLock.withLock {
            val registered = stores[eventType]?.mode
            if (registered == mode) return
            if (registered != null) throw ModeConflictException(eventType, registered)
            require(!isReserved(eventType)) { "An event type cannot be named $eventType" }

            // the queue may exist from before event types were registered
            val family = families[eventType]
                ?: db.createColumnFamily(ColumnFamilyDescriptor(eventType.encodeToByteArray()))
            families += eventType to family

            val types = families.getValue(EVENT_TYPES_FAMILY)
            db.put(types, writeOptions, eventType.encodeToByteArray(), mode.name.encodeToByteArray())
            stores += eventType to EventStore(db, family, mode)
        }
        logger.info("Registered event type {} with mode {}", eventType, mode)
    }

    /** Reads the mode of every event type that is registered. */
    private fun loadEventTypes(): Map<String, EventMode> =
        db.newIterator(families.getValue(EVENT_TYPES_FAMILY)).use { iter ->
            val types = mutableMapOf<String, EventMode>()
            iter.seekToFirst()
            while (iter.isValid) {
                types[iter.key().decodeToString()] = EventMode.valueOf(iter.value().decodeToString())
                iter.next()
            }
            iter.status()
            types
        }

    /**
     * Adds an event to the back of the queue.
     *
     * @param event the event type
     * @param id the id of the event
     * @param message the message of the event
     * @param contact where the producer of the event may be contacted, if it provided an address
     */
    fun push(event: String, id: Uuid, message: String, contact: String? = null) =
        stores.getValue(event).push(listOf(id to message), contact)

    /**
     * Adds many events to the back of the queue in one write.
     *
     * @param event the event type
     * @param messages the id and message of each event
     */
    fun pushAll(event: String, messages: List<Pair<Uuid, String>>) = stores.getValue(event).push(messages)

    /**
     * Takes the oldest available event, hiding it from other borrows. It becomes available again if it is not marked
     * as received within [Config.Durations.maxNoReceiveDuration].
     *
     * @param event the event type
     * @return the borrowed event, or null if there is none
     */
    fun borrow(event: String): BorrowedEvent? = stores.getValue(event).borrow(Config.get().durations)

    /**
     * Marks a borrowed event as received, so that it stays hidden for [Config.Durations.maxProcessingDuration] from
     * the first time this is called.
     *
     * @param event the event type
     * @param borrowed the event to mark
     * @return false if it was removed or has since been borrowed again
     */
    fun markReceived(event: String, borrowed: BorrowedEvent): Boolean = stores.getValue(event).markReceived(borrowed)

    /**
     * Removes a finished event.
     *
     * @param event the event type
     * @param borrowed the event to remove
     * @return false if it was already removed or has since been borrowed again
     */
    fun pop(event: String, borrowed: BorrowedEvent): Boolean = stores.getValue(event).pop(borrowed)

    /**
     * Like the other pop, but finds the borrowed event by id and so cannot tell if it was borrowed again.
     *
     * @param id the id of the event to remove
     * @return false if it is not borrowed
     */
    fun pop(id: Uuid): Boolean = complete(id) != null

    /**
     * Like [pop], but also finds the type of the event.
     *
     * @param id the id of the event to remove
     * @return the type of the removed event, or null if it is not borrowed
     */
    fun complete(id: Uuid): String? = stores.entries.find { it.value.pop(id) }?.key
}
