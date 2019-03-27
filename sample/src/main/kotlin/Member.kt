import java.time.LocalDateTime

data class Member(
        val id: Int,
        val name: String?,
        val createdAt: LocalDateTime)
