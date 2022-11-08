package task.scheduler.kotlin.task

import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class Task(
    val taskId: String = UUID.randomUUID().toString(),
    val taskType: String, val ttlInSeconds: ULong,
    val payload: String
)