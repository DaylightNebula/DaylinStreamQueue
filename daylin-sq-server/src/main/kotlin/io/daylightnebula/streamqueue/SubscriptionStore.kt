package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.Subscription
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.SerializationException
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import kotlin.uuid.Uuid
import kotlin.uuid.getUuid
import kotlin.uuid.putUuid

/**
 * The active subscriptions table, kept in its own column family. Keys are the id, values are the subscription's JSON.
 *
 * @param db the database to save subscriptions in
 * @param family the column family of the table
 */
class SubscriptionStore(private val db: RocksDB, private val family: ColumnFamilyHandle) {

    private val logger = LoggerFactory.getLogger(SubscriptionStore::class.java)

    /**
     * Reads every saved subscription. Ones that cannot be read, such as those saved before subscriptions had a mode, are
     * removed and their subscribers have to subscribe again.
     *
     * @return the saved subscriptions by id
     */
    fun load(): Map<Uuid, Subscription> = db.newIterator(family).use { iter ->
        val subscriptions = mutableMapOf<Uuid, Subscription>()
        iter.seekToFirst()
        while (iter.isValid) {
            val id = ByteBuffer.wrap(iter.key()).getUuid()
            try {
                subscriptions[id] = streamQueueJson.decodeFromString(iter.value().decodeToString())
            } catch (e: SerializationException) {
                logger.warn("Removed subscription {} as it could not be read: {}", id, e.message)
                remove(id)
            }
            iter.next()
        }
        iter.status()
        subscriptions
    }

    /**
     * Saves a subscription and removes the one it replaces in the same write.
     *
     * @param id the id of the new subscription
     * @param subscription the subscription to save
     * @param replaced the id of the subscription to remove, if there is one
     */
    fun save(id: Uuid, subscription: Subscription, replaced: Uuid?) {
        WriteBatch().use { batch ->
            replaced?.let { batch.delete(family, key(it)) }
            batch.put(family, key(id), streamQueueJson.encodeToString(subscription).encodeToByteArray())
            db.write(Database.writeOptions, batch)
        }
    }

    /**
     * Removes a subscription, does nothing if it is not saved.
     *
     * @param id the id of the subscription to remove
     */
    fun remove(id: Uuid) = db.delete(family, Database.writeOptions, key(id))

    private fun key(id: Uuid) = ByteBuffer.allocate(16).putUuid(id).array()
}
