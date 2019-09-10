package io.y2k.sqlitesync

import android.database.sqlite.SQLiteDatabase

@Suppress("unused")
class AndroidDatabaseProvider(name: String, private val db: SQLiteDatabase) :
    DatabaseProvider {

    init {
        db.execSQL("CREATE TABLE IF NOT EXISTS $name (key TEXT, value TEXT, action TEXT)")
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
