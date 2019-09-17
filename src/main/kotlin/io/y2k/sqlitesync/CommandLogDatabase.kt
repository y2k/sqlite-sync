package io.y2k.sqlitesync

import io.y2k.sqlitesync.CommandLogDatabase.State
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class CommandLogDatabase<State, Command>(
    initState: State,
    vararg materializedViewGenerators: (Command, State) -> State
) {

    private data class State<S, C>(val state: S, val log: List<C> = emptyList())

    private val materializedViewGenerators = materializedViewGenerators.toList()
    private val state = atomic(State<State, Command>(initState))

    fun <T> rewriteLog(f: (List<Command>) -> Pair<List<Command>, T>): T =
        state.updateWithResult {
            val (updatedLog, result) = f(it.log)
            it.copy(log = updatedLog) to result
        }

    fun <T> update(f: (State) -> Pair<List<Command>, T>): T =
        state.updateWithResult {
            val (newCommands, r) = f(it.state)
            val newStore =
                materializedViewGenerators.fold(it.state) { acc, function ->
                    newCommands.fold(acc) { acc2, cmd ->
                        function(cmd, acc2)
                    }
                }
            State(newStore, it.log + newCommands) to r
        }

    private inline fun <T, R> AtomicRef<T>.updateWithResult(f: (T) -> Pair<T, R>): R {
        while (true) {
            val cur = value
            val (upd, result) = f(cur)
            if (compareAndSet(cur, upd)) return result
        }
    }
}

fun <S, C> CommandLogDatabase<S, C>.updateOne(f: (S) -> C) =
    update { listOf(f(it)) to Unit }

fun <S, C, T> CommandLogDatabase<S, C>.read(f: (S) -> T): T =
    update { emptyList<C>() to f(it) }
