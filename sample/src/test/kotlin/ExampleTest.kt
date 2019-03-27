import io.github.hj_lee.jdbcextensions.selectAll
import org.junit.Before
import org.junit.Test
import java.sql.ResultSet
import kotlin.test.assertEquals


class ExampleTest {

    val example = Example()

    val toMember: (ResultSet) -> Member = { rs ->
        Member(rs.getInt("id"),
                rs.getString("name"),
                rs.getTimestamp("created_at").toLocalDateTime())
    }

    @Before
    fun prepare() {
        example.init()
    }

    @Test
    fun sample() {
        val ds = example.ds
        val members = ds.selectAll("select id, name, created_at from members", block = toMember)
        assertEquals(2, members.size)
    }

}
