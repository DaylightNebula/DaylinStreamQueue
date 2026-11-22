package io.daylightnebula.streamqueue

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

object Database {

    private const val PATH = "data"
    private const val READY: Byte = 0
    private const val TAKEN: Byte = 1
    private const val RECEIVED: Byte = 2

    // where id and recvTime start in a value
    private const val ID_OFFSET = 8
    private const val RECV_TIME_OFFSET = 24

    private const val CLEANUP_BATCH = 1_000
    private val cleanupInterval = 1.minutes
    private val logger = LoggerFactory.getLogger(Database::class.java)

    lateinit var writeOptions: WriteOptions
    lateinit var db: RocksDB
    lateinit var stores: Map<String, EventStore>

    data class BorrowedEvent(
        val seq: Long,
        val takenAt: Long,
        val id: Uuid,
        val recvTime: Instant,
        val message: String
    )

    /**
     * One queue, kept in its own column family. Values are takenAt, id, recvTime, then the message. Received events
     * have receivedAt in front of that.
     */
    class EventStore(private val db: RocksDB, private val family: ColumnFamilyHandle) {
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

        fun push(messages: List<Pair<Uuid, String>>) {
            lock.withLock {
                val now = Clock.System.now().toEpochMilliseconds()
                WriteBatch().use { batch ->
                    messages.forEachIndexed { i, (id, message) ->
                        batch.put(family, key(READY, tail + i), encode(id, now, message))
                    }
                    db.write(writeOptions, batch)
                }
                tail += messages.size
            }
        }

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

        fun markReceived(borrowed: BorrowedEvent): Boolean = lock.withLock { markReceived(find(borrowed)) }

        fun markReceived(id: Uuid): Boolean = lock.withLock { markReceived(heldById[id]) }

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

        fun pop(borrowed: BorrowedEvent): Boolean = lock.withLock { pop(find(borrowed)) }

        fun pop(id: Uuid): Boolean = lock.withLock { pop(heldById[id]) }

        private fun pop(current: Held?): Boolean {
            if (current == null) return false
            db.delete(family, writeOptions, keyOf(current))
            release(current)
            return true
        }

        /** Removes events past [Config.Durations.maxTimeInQueue], except ones a consumer is still allowed to hold. */
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

        private fun encode(id: Uuid, recvTime: Long, message: String): ByteArray {
            val bytes = message.encodeToByteArray()
            return ByteBuffer.allocate(32 + bytes.size)
                .putLong(0)
                .putUuid(id)
                .putLong(recvTime)
                .put(bytes)
                .array()
        }

        private fun decode(seq: Long, value: ByteArray): BorrowedEvent {
            val buffer = ByteBuffer.wrap(value)
            return BorrowedEvent(
                seq = seq,
                takenAt = buffer.getLong(),
                id = buffer.getUuid(),
                recvTime = Instant.fromEpochMilliseconds(buffer.getLong()),
                message = value.decodeToString(startIndex = 32)
            )
        }
    }

    fun init() {
        // setup Rock DB
        RocksDB.loadLibrary()
        writeOptions = WriteOptions().setSync(false)

        // pull all possible event ids
        val events = Config.get().events.keys

        // RocksDB must open every existing column family, including ones no longer in the config
        val existing = Options().use { RocksDB.listColumnFamilies(it, PATH) }.map { it.decodeToString() }
        val names = (existing + "default" + events).distinct()

        val handles = mutableListOf<ColumnFamilyHandle>()
        val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, PATH, names.map { ColumnFamilyDescriptor(it.encodeToByteArray()) }, handles)
        stores = names.zip(handles).filter { (name, _) -> name in events }.associate { (name, family) ->
            name to EventStore(db, family)
        }

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

    /** Adds an event to the back of the queue. */
    fun push(event: String, id: Uuid, message: String) = pushAll(event, listOf(id to message))

    /** Adds many (id, message) events to the back of the queue in one write. */
    fun pushAll(event: String, messages: List<Pair<Uuid, String>>) = stores.getValue(event).push(messages)

    /**
     * Takes the oldest available event, hiding it from other borrows. It becomes available again if it is not marked
     * as received within [Config.Durations.maxNoReceiveDuration].
     */
    fun borrow(event: String): BorrowedEvent? = stores.getValue(event).borrow(Config.get().durations)

    /**
     * Marks a borrowed event as received, so that it stays hidden for [Config.Durations.maxProcessingDuration] from
     * the first time this is called. Returns false if it was removed or has since been borrowed again.
     */
    fun markReceived(event: String, borrowed: BorrowedEvent): Boolean = stores.getValue(event).markReceived(borrowed)

    /** Removes a finished event. Returns false if it was already removed or has since been borrowed again. */
    fun pop(event: String, borrowed: BorrowedEvent): Boolean = stores.getValue(event).pop(borrowed)

    /** Like the other markReceived, but finds the borrowed event by id and so cannot tell if it was borrowed again. */
    fun markReceived(id: Uuid): Boolean = stores.values.any { it.markReceived(id) }

    /** Like the other pop, but finds the borrowed event by id and so cannot tell if it was borrowed again. */
    fun pop(id: Uuid): Boolean = stores.values.any { it.pop(id) }
}
