package io.y2k.sqlitesync

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class CommandLogDatabase<Store, Command>(
    initState: Store,
    vararg fs: (Command, Store) -> Store
) {

    private data class State<S, C>(val state: S, val log: List<C> = emptyList())

    private val fs = fs.toList()
    private val state = atomic(State<Store, Command>(initState))

    fun <T> update(f: (Store) -> Pair<List<Command>, T>): T =
        state.updateWithResult {
            val (newCommands, r) = f(it.state)
            val newStore =
                fs.fold(it.state) { acc, function ->
                    newCommands.fold(acc) { acc2, cmd ->
                        function(cmd, acc2)
                    }
                }
            State(newStore, it.log + newCommands) to r
        }

    private inline fun <T, R> AtomicRef<T>.updateWithResult(function: (T) -> Pair<T, R>): R {
        while (true) {
            val cur = value
            val (upd, r) = function(cur)
            if (compareAndSet(cur, upd)) return r
        }
    }
}

fun <S, C> CommandLogDatabase<S, C>.updateOne(f: (S) -> C) =
    update { listOf(f(it)) to Unit }

fun <S, C, T> CommandLogDatabase<S, C>.read(f: (S) -> T): T =
    update { emptyList<C>() to f(it) }
