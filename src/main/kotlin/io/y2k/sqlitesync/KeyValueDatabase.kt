package io.y2k.sqlitesync

import android.database.sqlite.SQLiteDatabase
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.collections.immutable.persistentListOf

sealed class Command<out T> {
    class Update<T>(val key: String, val value: T) : Command<T>()
    class Remove(val key: String) : Command<Nothing>()
}

class AndroidDatabaseProvider(private val name: String, private val db: SQLiteDatabase) : DatabaseProvider {

    init {
        db.execSQL("CREATE TABLE $name (key TEXT, value TEXT, action TEXT)")
    }

    override suspend fun useTransaction(f: () -> Unit) {
        db.beginTransaction()
        try {
            f()
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    override suspend fun <T : Any> query(sql: String, mapTo: (Row) -> T): List<T> {
        return db.rawQuery(sql, emptyArray()).use { c ->
            List(c.count) {
                c.moveToPosition(it)
                mapTo(object : Row {
                    override fun getString(column: String): String? = c.getString(c.getColumnIndex(column))
                    override fun getBoolean(column: String): Boolean = TODO()
                    override fun getLong(column: String): Long = TODO()
                })
            }
        }
    }

    override fun execSql(sql: String, vararg data: String) = db.execSQL(sql, data)
}

interface DatabaseProvider {
    suspend fun useTransaction(f: () -> Unit)
    suspend fun <T : Any> query(sql: String, mapTo: (Row) -> T): List<T>
    fun execSql(sql: String, vararg data: String)
}

interface Row {
    fun getString(column: String): String?
    fun getBoolean(column: String): Boolean
    fun getLong(column: String): Long
}

class KeyValueDatabase<T : Any>(
    private val db: DatabaseProvider,
    private val table: String,
    private val serialize: (T) -> String,
    private val deserialize: (String) -> T
) {

    private val log = atomic(persistentListOf<Command<T>>())
    private val store = atomic(persistentHashMapOf<String, T>())

    private suspend fun init() {
        db.query("SELECT * FROM $table") {
            Record(it.getString("action")!!, it.getString("key")!!, it.getString("value"))
        }.forEach {
            log.update { xs ->
                xs.add(
                    when (it.action) {
                        "update" -> Command.Update(it.key, deserialize(it.value!!))
                        "delete" -> Command.Remove(it.key)
                        else -> error("$it")
                    }
                )
            }
        }
        reloadStore()
    }

    suspend fun update(f: (PersistentMap<String, T>) -> List<Command<T>>) {
        f(store.value).forEach { x ->
            log.update { it.add(x) }
        }
        reloadStore()
        reloadDatabase()
    }

    private var currentOffset = 0

    private suspend fun reloadDatabase() {
        val snapshot = log.value
        db.useTransaction {
            snapshot.drop(currentOffset).forEach {
                when (it) {
                    is Command.Update ->
                        db.execSql(
                            "INSERT INTO $table (action, key, value) VALUES('update', ?, ?)",
                            it.key, serialize(it.value)
                        )
                    is Command.Remove ->
                        db.execSql(
                            "INSERT INTO $table (action, key) VALUES('delete', ?)",
                            it.key
                        )
                }
            }
            currentOffset = snapshot.size
        }
    }

    private var currentStoreOffset = 0

    private fun reloadStore() {
        val snapshot = log.value
        store.value = snapshot.drop(currentStoreOffset).asSequence().fold(persistentHashMapOf(),
            { xs, x ->
                when (x) {
                    is Command.Update -> xs.put(x.key, x.value)
                    is Command.Remove -> xs.remove(x.key)
                }
            }
        )
        currentStoreOffset = snapshot.size
    }

    val snapshot get() = store.value

    private data class Record(val action: String, val key: String, val value: String?)

    companion object {
        suspend fun <T : Any> make(
            db: DatabaseProvider, table: String, serialize: (T) -> String, deserialize: (String) -> T
        ) = KeyValueDatabase(db, table, serialize, deserialize).apply { init() }
    }
}
