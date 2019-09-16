package io.y2k.sqlitesync

import io.y2k.sqlitesync.CommandLogDatabaseTests.Command.*
import kotlin.test.Test
import kotlin.test.assertEquals

class CommandLogDatabaseTests {

    @Test
    fun `save and restore log test`() {
        val logs = run {
            val store = CommandLogDatabase(AppStore(), ::convertToState, ::convertToUser)

            store.update { List(3) { NewSlackRecord("user1", "compose #$it") } to Unit }
            assertEquals(3, store.updateLog { it to it.size })

            val logs = store.updateLog { it to it }
            assertEquals(3, store.updateLog { it to it.size })
            // Save to disk
            store.updateLog { it.drop(logs.size) to Unit }
            assertEquals(0, store.updateLog { it to it.size })
            logs
        }

        run {
            val store = CommandLogDatabase(AppStore(), ::convertToState, ::convertToUser)
            assertEquals(AppStore(), store.read { it })
        }
    }

    @Test
    fun `log test`() {
        val store = CommandLogDatabase(AppStore(), ::convertToState, ::convertToUser)

        store.update { List(3) { NewSlackRecord("user1", "compose #$it") } to Unit }
        assertEquals(3, store.updateLog { it to it.size })

        store.update { List(3) { NewSlackRecord("user1", "compose #$it") } to Unit }
        assertEquals(6, store.updateLog { it to it.size })

        store.updateLog { emptyList<Command>() to Unit }
        assertEquals(0, store.updateLog { it to it.size })
    }

    @Test
    fun test() {
        val store = CommandLogDatabase(AppStore(), ::convertToState, ::convertToUser)

        store.updateOne { NewSlackRecord("user1", "compose") }
        assertEquals(1, store.read { it.records.size })

        store.update {
            listOf(
                NewUrlRecord("user1", "https://kotlinlang.slack.com/feed"),
                NewUrlRecord("user1", "https://kotlinlang.slack.com/russian")
            ) to Unit
        }
        assertEquals(3, store.read { it.records.size })
        assertEquals(0, store.read { it.users.size })

        store.updateOne { NewUser("user1") }
        assertEquals(3, store.read { it.records.size })
        assertEquals(1, store.read { it.users.size })

        store.updateOne { db ->
            RemoveRecord("user1", db.records.first { it.url.endsWith("/compose") }.url)
        }
        assertEquals(2, store.read { it.records.size })
        assertEquals(1, store.read { it.users.size })
    }

    private fun convertToState(cmd: Command, state: AppStore): AppStore = when (cmd) {
        is NewSlackRecord -> state.copy(
            records = state.records + Record(cmd.userId, "https://kotlinlang.slack.com/${cmd.channel}")
        )
        is NewUrlRecord -> state.copy(records = state.records + Record(cmd.userId, cmd.url))
        is RemoveRecord -> state.copy(
            records = state.records.filterNot { it.userId == cmd.userId && it.url == cmd.url })
        else -> state
    }

    private fun convertToUser(cmd: Command, state: AppStore): AppStore = when (cmd) {
        is NewUser -> state.copy(users = state.users + cmd.userId)
        else -> state
    }

    data class Record(val userId: String, val url: String)
    data class AppStore(val records: List<Record> = emptyList(), val users: List<String> = emptyList())
    sealed class Command {
        data class NewUser(val userId: String) : Command()
        data class NewSlackRecord(val userId: String, val channel: String) : Command()
        data class NewUrlRecord(val userId: String, val url: String) : Command()
        data class RemoveRecord(val userId: String, val url: String) : Command()
    }
}
