package io.github.hj_lee.jdbcextensions

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource

////////////////////////////////////////////


fun ResultSet.toAnyList() =
        (1..this.metaData.columnCount).map {
            this.getObject(it)
        }

fun ResultSet.asSequence() = generateSequence { if (this.next()) this else null }

fun <T> ResultSet.selectFirst(block: (ResultSet) -> T) = this.asSequence().map(block).firstOrNull()

fun ResultSet.selectFirst() = this.selectFirst(ResultSet::toAnyList)

fun ResultSet.selectEach(block: (ResultSet) -> Unit) = this.asSequence().forEach(block)

fun <T> ResultSet.selectAll(block: (ResultSet) -> T) = this.asSequence().map(block).toList()

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
    return this.addBatch()
}

fun <T> PreparedStatement.selectFirst(vararg params: Any?, block: (ResultSet) -> T) =
        this.select(*params).use { it.selectFirst(block) }

fun PreparedStatement.selectFirst(vararg params: Any?) =
        this.select(*params).use { it.selectFirst() }

fun PreparedStatement.selectEach(vararg params: Any?, block: (ResultSet) -> Unit) =
        this.select(*params).use { it.selectEach(block) }

fun <T> PreparedStatement.selectAll(vararg params: Any?, block: (ResultSet) -> T): List<T> =
        this.select(*params).use { it.selectAll(block) }

fun PreparedStatement.selectAll(vararg params: Any?): List<List<Any?>> =
        this.select(*params).use { it.selectAll() }


////////////////////////////////////////////////////////////////

fun Statement.select(sql: String): ResultSet = this.executeQuery(sql)

fun <T> Statement.selectFirst(sql: String, block: (ResultSet) -> T) =
        this.select(sql).use { it.selectFirst(block) }

fun Statement.selectFirst(sql: String) =
        this.select(sql).use { it.selectFirst() }


fun Statement.selectEach(sql: String, block: (ResultSet) -> Unit) =
        this.select(sql).use { it.selectEach(block) }

fun <T> Statement.selectAll(sql: String, block: (ResultSet) -> T): List<T> =
        this.select(sql).use { it.selectAll(block) }

fun Statement.selectAll(sql: String): List<List<Any?>> =
        this.select(sql).use { it.selectAll() }

////////////////////////////////////////////////////////////////

private fun <T> Connection.execute(sql: String, params: Array<out Any?>, stmtAction: (Statement) -> T, prepAction: (PreparedStatement) -> T) =
        if (params.isEmpty()) {
            this.createStatement().use { stmtAction(it) }
        } else {
            this.prepareStatement(sql).use { prepAction(it) }
        }

fun Connection.update(sql: String, vararg params: Any?) =
        this.execute(sql, params, stmtAction = { it.executeUpdate(sql) }, prepAction = { it.update(*params) })

fun <T> Connection.selectFirst(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
        this.execute(sql, params, stmtAction = { it.selectFirst(sql, block) }, prepAction = { it.selectFirst(*params, block = block) })

fun Connection.selectFirst(sql: String, vararg params: Any?) =
        this.execute(sql, params, stmtAction = { it.selectFirst(sql) }, prepAction = { it.selectFirst(*params) })

fun Connection.selectEach(sql: String, vararg params: Any?, block: (ResultSet) -> Unit) =
        this.execute(sql, params, stmtAction = { it.selectEach(sql, block) }, prepAction = { it.selectEach(*params, block = block) })

fun <T> Connection.selectAll(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
        this.execute(sql, params, stmtAction = { it.selectAll(sql, block) }, prepAction = { it.selectAll(*params, block = block) })

fun Connection.selectAll(sql: String, vararg params: Any?) =
        this.execute(sql, params, stmtAction = { it.selectAll(sql) }, prepAction = { it.selectAll(*params) })

////////////////////////////////////////////////////////////////

fun DataSource.update(sql: String, vararg params: Any?) =
        this.connection.use { it.update(sql, *params) }

fun <T> DataSource.selectFirst(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
        this.connection.use { it.selectFirst(sql, *params, block = block) }

fun DataSource.selectFirst(sql: String, vararg params: Any?) =
        this.connection.use { it.selectFirst(sql, *params) }

fun DataSource.selectEach(sql: String, vararg params: Any?, block: (ResultSet) -> Unit) =
        this.connection.use { it.selectEach(sql, *params, block = block) }

fun <T> DataSource.selectAll(sql: String, vararg params: Any?, block: (ResultSet) -> T) =
        this.connection.use { it.selectAll(sql, *params, block = block) }

fun DataSource.selectAll(sql: String, vararg params: Any?) =
        this.connection.use { it.selectAll(sql, *params) }
