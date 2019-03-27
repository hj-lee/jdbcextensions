package io.github.hj_lee.jdbcextensions

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource

////////////////////////////////////////////

fun ResultSet.toAnyList(): List<Any?> =
    (1..this.metaData.columnCount).map {
        this.getObject(it)
    }

fun ResultSet.asSequence() =
    generateSequence {
        if (this.next()) this
        else null
    }

inline fun <T> ResultSet.selectFirst(block: (ResultSet) -> T) = if (this.next()) block(this) else null

fun ResultSet.selectFirst() = this.selectFirst(ResultSet::toAnyList)

// Below definitions are concise, but use of sequence is not necessary
// inline fun ResultSet.selectEach(block: (ResultSet) -> Unit) = this.asSequence().forEach(block)
// fun <T> ResultSet.selectAll(block: (ResultSet) -> T) = this.asSequence().map(block).toList()

inline fun ResultSet.selectEach(block: (ResultSet) -> Unit) {
    while (this.next()) {
        block(this)
    }
}

inline fun <T> ResultSet.selectAll(block: (ResultSet) -> T): List<T> {
    val list = mutableListOf<T>()
    while (this.next()) {
        list += block(this)
    }
    return list
}

fun ResultSet.selectAll() = this.selectAll(ResultSet::toAnyList)

///////////////////////////////////////////////

fun PreparedStatement.setObjects(vararg params: Any?) {
    params.forEachIndexed { index, v ->
        this.setObject(index + 1, v)
    }
}

fun PreparedStatement.update(vararg params: Any?): Int {
    this.setObjects(*params)
    return this.executeUpdate()
}

fun PreparedStatement.select(vararg params: Any?): ResultSet {
    this.setObjects(*params)
    return this.executeQuery()
}

fun PreparedStatement.addBatchItem(vararg params: Any?) {
    this.setObjects(*params)
    this.addBatch()
}

inline fun <T> PreparedStatement.selectFirst(vararg params: Any?, block: (ResultSet) -> T) =
    this.select(*params).use { it.selectFirst(block) }

fun PreparedStatement.selectFirst(vararg params: Any?) =
    this.select(*params).use { it.selectFirst() }

inline fun PreparedStatement.selectEach(vararg params: Any?, block: (ResultSet) -> Unit) =
    this.select(*params).use { it.selectEach(block) }

inline fun <T> PreparedStatement.selectAll(vararg params: Any?, block: (ResultSet) -> T): List<T> =
    this.select(*params).use { it.selectAll(block) }

fun PreparedStatement.selectAll(vararg params: Any?): List<List<Any?>> =
    this.select(*params).use { it.selectAll() }


////////////////////////////////////////////////////////////////

fun Statement.select(sql: String): ResultSet = this.executeQuery(sql)

inline fun <T> Statement.selectFirst(sql: String, block: (ResultSet) -> T) =
    this.select(sql).use { it.selectFirst(block) }

fun Statement.selectFirst(sql: String) =
    this.select(sql).use { it.selectFirst() }

inline fun Statement.selectEach(sql: String, block: (ResultSet) -> Unit) =
    this.select(sql).use { it.selectEach(block) }

inline fun <T> Statement.selectAll(sql: String, block: (ResultSet) -> T): List<T> =
    this.select(sql).use { it.selectAll(block) }

fun Statement.selectAll(sql: String): List<List<Any?>> =
    this.select(sql).use { it.selectAll() }

////////////////////////////////////////////////////////////////

inline fun <T> Connection.useStatement(block: (Statement) -> T) = this.createStatement().use(block)
inline fun <T> Connection.usePreparedStatement(sql: String, block: (PreparedStatement) -> T) =
    this.prepareStatement(sql).use(block)

fun Connection.execute(sql: String) = this.useStatement { it.execute(sql) }

fun Connection.update(sql: String) = this.useStatement { it.executeUpdate(sql) }
fun Connection.update(sql: String, vararg params: Any?) = this.usePreparedStatement(sql) { it.update(*params) }

inline fun <T> Connection.selectFirst(sql: String, block: (ResultSet) -> T) =
    this.useStatement { it.selectFirst(sql, block) }

inline fun <T> Connection.selectFirst(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.usePreparedStatement(sql) { it.selectFirst(*params, block = block) }

fun Connection.selectFirst(sql: String) = this.useStatement { it.selectFirst(sql) }
fun Connection.selectFirst(sql: String, vararg params: Any?) =
    this.usePreparedStatement(sql) { it.selectFirst(*params) }

inline fun Connection.selectEach(sql: String, block: (ResultSet) -> Unit) =
    this.useStatement { it.selectEach(sql, block) }

inline fun Connection.selectEach(sql: String, vararg params: Any?, block: (ResultSet) -> Unit) =
    this.usePreparedStatement(sql) { it.selectEach(*params, block = block) }

inline fun <T> Connection.selectAll(sql: String, block: (ResultSet) -> T) =
    this.useStatement { it.selectAll(sql, block) }

inline fun <T> Connection.selectAll(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.usePreparedStatement(sql) { it.selectAll(*params, block = block) }

fun Connection.selectAll(sql: String) = this.useStatement { it.selectAll(sql) }
fun Connection.selectAll(sql: String, vararg params: Any?) =
    this.usePreparedStatement(sql) { it.selectAll(*params) }

inline fun <T> Connection.useResultSet(sql: String, block: (ResultSet) -> T) =
    this.useStatement { it.select(sql).use(block) }

inline fun <T> Connection.useResultSet(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.usePreparedStatement(sql) { it.select(*params).use(block) }

////////////////////////////////////////////////////////////////

fun DataSource.update(sql: String, vararg params: Any?) =
    this.connection.use { it.update(sql, *params) }

fun DataSource.execute(sql: String) = this.connection.use { it.execute(sql) }

inline fun <T> DataSource.selectFirst(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.connection.use { it.selectFirst(sql, *params, block = block) }

fun DataSource.selectFirst(sql: String, vararg params: Any?) =
    this.connection.use { it.selectFirst(sql, *params) }

inline fun DataSource.selectEach(sql: String, vararg params: Any?, block: (ResultSet) -> Unit) =
    this.connection.use { it.selectEach(sql, *params, block = block) }

inline fun <T> DataSource.selectAll(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.connection.use { it.selectAll(sql, *params, block = block) }

fun DataSource.selectAll(sql: String, vararg params: Any?) =
    this.connection.use { it.selectAll(sql, *params) }

inline fun <T> DataSource.useResultSet(sql: String, block: (ResultSet) -> T) =
    this.connection.use { it.useResultSet(sql, block) }

inline fun <T> DataSource.useResultSet(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
    this.connection.use { it.useResultSet(sql, *params, block = block) }

///////////////////////////////////////////////////////////////////

inline fun PreparedStatement.batch(block: (PreparedStatement) -> Unit): IntArray? {
    block(this)
    return executeBatch()
}

inline fun <T> Connection.transaction(block: (Connection) -> T): T {
    try {
        autoCommit = false
        return block(this)
    } finally {
        autoCommit = true // transaction will be committed if autoCommit hasn't been change by block
    }
}

