package io.y2k.sqlitesync

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.persistentListOf
import kotlinx.coroutines.delay

sealed class Command<T> {
    class Add<T>(val value: T) : Command<T>()
    class Remove<T>(val value: T) : Command<T>()
    class Update<T>(val value: T) : Command<T>()
}

interface SQLiteDatabase {
    suspend fun useTransaction(f: () -> Unit)
    suspend fun <T : Any> query(sql: String, mapTo: (Row) -> T): List<T>
}

interface Row {
    fun getString(column: String): String
    fun getBoolean(column: String): Boolean
    fun getLong(column: String): Long
}

class Library<T : Any>(private val db: SQLiteDatabase, private val mapTo: (Row) -> T, private val table: String) {

    private val log = atomic(persistentListOf<Command<T>>())
    private val store = atomic(persistentListOf<T>())

    suspend fun init() {
        db.query("SELECT * FROM $table") { mapTo(it) }
            .forEach { x -> log.update { it.add(Command.Add(x)) } }
        reloadStore()
    }

    suspend fun update(f: (PersistentList<T>) -> List<Command<T>>) {
        f(store.value).forEach { x ->
            log.update { it.add(x) }
        }

        reloadStore()
        reloadDatabase()
    }

    private suspend fun reloadDatabase() {
        var currentOffset = 0
        while (true) {
            val snapshot = log.value
            db.useTransaction {
                snapshot.drop(currentOffset).forEach {
                    when (it) {
                        is Command.Add -> TODO("INSERT ... INTO words ...")
                        is Command.Remove -> TODO("DELETE FROM words")
                        is Command.Update -> TODO("UPDATE")
                    }
                }
                currentOffset = snapshot.size
            }
            delay(100)
        }
    }

    private fun reloadStore() {
        store.value = log.value.asSequence().fold(persistentListOf(),
            { xs, x ->
                when (x) {
                    is Command.Add -> xs.add(x.value)
                    is Command.Remove -> TODO()
                    is Command.Update -> TODO()
                }
            }
        )
    }

    fun snapshot(): PersistentList<T> = store.value
}
