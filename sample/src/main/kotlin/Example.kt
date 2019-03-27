import com.zaxxer.hikari.HikariDataSource
import io.github.hj_lee.jdbcextensions.update
import io.github.hj_lee.jdbcextensions.usePreparedStatement
import io.github.hj_lee.jdbcextensions.useStatement
import java.time.LocalDateTime

class Example {
    val ds = HikariDataSource()

    init {
        ds.jdbcUrl = "jdbc:h2:mem:hello"
        ds.username = "user"
        ds.password = "pass"
    }

    fun init() {
        ds.connection.use { connection ->
            connection.useStatement { statement ->
                statement.execute("""
                create table members (
                  id serial not null primary key,
                  name varchar(64),
                  created_at timestamp not null
                )
                """.trimIndent())
                val insertQuery = "insert into members (name,  created_at) values (?, ?)"
                connection.usePreparedStatement(insertQuery) { pStmt ->
                    listOf("Alice", "Bob").forEach { name ->
                        pStmt.update(name, LocalDateTime.now())
                    }
                }
            }
        }
    }
}
