package io.y2k.sqlitesync

import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTests {

    @Test(timeout = 5_000)
    fun test() = runBlocking {
        val db = TestDatabase()

        var lib = makeLibrary(db)

        lib.update { List(3) { Command.Add("1", Item(false, "1s", "1t", 1L)) } }
        assertEquals(1, lib.snapshot.size)

        lib = makeLibrary(db)
        assertEquals(1, lib.snapshot.size)

        lib.update { listOf(Command.Remove(it.toList().first().first)) }
        lib = makeLibrary(db)
        assertEquals(0, lib.snapshot.size)
    }

    private suspend fun makeLibrary(db: SQLiteDatabase): Library<Item> {
        return Library.make(
            db, "history",
            { "${it.favorite}|${it.source}|${it.target}|${it.id}" },
            {
                val (a, b, c, d) = it.split('|')
                Item(a.toBoolean(), b, c, d.toLong())
            }
        )
    }

    data class Item(
        val favorite: Boolean,
        val source: String,
        val target: String,
        val id: Long
    )
}

private class TestDatabase : SQLiteDatabase {

    private val conn: Connection

    init {
        Class.forName("org.sqlite.JDBC")
        conn = DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute("CREATE TABLE history (key TEXT, value TEXT, action TEXT)")
    }

    override fun execSql(sql: String, vararg data: String) {
        val stat = conn.prepareStatement(sql)
        data.forEachIndexed { index, value ->
            stat.setString(index + 1, value)
        }

        stat.execute()
        stat.close()
    }

    override suspend fun useTransaction(f: () -> Unit): Unit = f()

    override suspend fun <T : Any> query(sql: String, mapTo: (Row) -> T): List<T> {
        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery(sql)
        if (resultSet.isClosed) return emptyList()
        return resultSet
            .toList { row ->
                mapTo(object : Row {
                    override fun getBoolean(column: String): Boolean = row.getBoolean(column)
                    override fun getLong(column: String): Long = row.getLong(column)
                    override fun getString(column: String): String? = row.getString(column)
                })
            }
    }

    fun <T : Any> ResultSet.toList(f: (ResultSet) -> T): List<T> =
        use {
            val r = ArrayList<T>()
            while (it.next()) r.add(f(it))
            r
        }
}
