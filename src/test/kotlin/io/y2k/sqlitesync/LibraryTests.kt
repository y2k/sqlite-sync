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
        val db = object : SQLiteDatabase {

            private val conn: Connection

            init {
                Class.forName("org.sqlite.JDBC")
                conn = DriverManager.getConnection("jdbc:sqlite::memory:")
                conn.createStatement().execute("CREATE TABLE foo (favorite BOOL, source TEXT, text target, id NUMBER)")
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
                            override fun getString(column: String): String = row.getString(column)
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

        val l = Library(
            db,
            {
                Item(
                    it.getBoolean("favorite"),
                    it.getString("source"),
                    it.getString("target"),
                    it.getLong("id")
                )
            },
            "foo"
        )

        l.init()

        l.update {
            listOf(Command.Add(Item(false, "1s", "1t", 1L)))
        }

        val snap = l.snapshot()

        assertEquals(1, snap.size)
    }

    data class Item(
        val favorite: Boolean,
        val source: String,
        val target: String,
        val id: Long
    )
}
