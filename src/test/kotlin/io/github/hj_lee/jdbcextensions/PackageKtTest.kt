package io.github.hj_lee.jdbcextensions

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import java.sql.*
import javax.sql.DataSource

internal class PackageKtTest {
    private val md = mock<ResultSetMetaData> {
        on { columnCount } doReturn 2
    }
    private val rs = mock<ResultSet> {
        on { metaData } doReturn md
        on { next() }.thenReturn(true, true, false)
    }
    private val pstmt = mock<PreparedStatement> {
        on { executeQuery() } doReturn rs
        on { executeUpdate() } doReturn 1
    }
    private val stmt = mock<Statement> {
        on { executeQuery(ArgumentMatchers.anyString()) } doReturn rs
        on { executeUpdate(ArgumentMatchers.anyString()) } doReturn 1
    }
    private val conn = mock<Connection> {
        on { createStatement() } doReturn stmt
        on { prepareStatement(ArgumentMatchers.anyString()) } doReturn pstmt
    }
    private val ds = mock<DataSource> {
        on { connection } doReturn conn
    }

    @Test
    fun statementSelectAll() {
        stmt.selectAll("select * from tbl")
        inOrder(stmt, rs) {
            verify(stmt).executeQuery(any())
            verify(rs, times(3)).next()
            verify(rs).close()
        }
    }

    @Test
    fun statementSelectFirst() {
        stmt.selectFirst("select * from tbl where id = 1")
        inOrder(stmt, rs) {
            verify(stmt).executeQuery(any())
            verify(rs).next()
            verify(rs).close()
            verify(stmt, never()).close()
        }
    }

    @Test
    fun preparedStatementSelectAll() {
        pstmt.selectAll(1)
        inOrder(pstmt, rs) {
            verify(pstmt).setObject(eq(1), any())
            verify(pstmt).executeQuery()
            verify(rs, times(3)).next()
            verify(rs).close()
        }
    }

    @Test
    fun preparedStatementSelectFirst() {
        pstmt.selectFirst(1)
        inOrder(pstmt, rs) {
            verify(pstmt).setObject(eq(1), any())
            verify(pstmt).executeQuery()
            verify(rs).next()
            verify(rs).close()
            verify(pstmt, never()).close()
        }
    }

    @Test
    fun connectionPreparedStatement() {
        conn.update("insert into tbl values (?, ?)", 1, "name")
        conn.selectFirst("select * from tbl where id = ?", 1)
        inOrder(conn, pstmt) {
            verify(conn).prepareStatement(any())
            verify(pstmt).setObject(1, 1)
            verify(pstmt).setObject(2, "name")
            verify(pstmt).executeUpdate()
            verify(pstmt).close()
            verify(conn).prepareStatement(any())
            verify(pstmt).executeQuery()
            verify(pstmt).close()
            verify(conn, never()).close()
        }
    }

    @Test
    fun connectionStatement() {
        conn.update("insert into tbl values (1, 'name')")
        conn.selectFirst("select * from tbl where id = 1")
        inOrder(conn, stmt) {
            verify(conn).createStatement()
            verify(stmt).executeUpdate(any())
            verify(stmt).close()
            verify(conn).createStatement()
            verify(stmt).executeQuery(any())
            verify(stmt).close()
            verify(conn, never()).close()
        }
    }

    @Test
    fun dataSourceConnection() {
        ds.update("insert into tbl values (?, ?)", 1, "name")
        ds.selectFirst("select * from tbl where id = ?", 1)
        inOrder(ds, conn) {
            verify(ds).connection
            verify(conn).prepareStatement(any())
            verify(conn).close()
            verify(ds).connection
            verify(conn).prepareStatement(any())
            verify(conn).close()
        }
    }
}