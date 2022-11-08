package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import task.scheduler.kotlin.messaging.Messaging
import java.nio.charset.StandardCharsets

interface Handler : AutoCloseable {
    suspend fun handleTask(taskType: String): Either<Throwable, Task>
}

class HandlerImpl(private val messagingConsumer: Messaging.Consumer, private val taskRepository: TaskRepository) :
    Handler {
    override suspend fun handleTask(taskType: String): Either<Throwable, Task> = either {
        val taskString = String(messagingConsumer.consume(taskType).bind(), StandardCharsets.UTF_8)
        Either.catch {
            Json.decodeFromString<Task>(taskString)
        }.bind()
    }

    override fun close() {
        messagingConsumer.close()
        taskRepository.close()
    }
}

fun handler(messagingConsumer: Messaging.Consumer, taskRepository: TaskRepository): Resource<HandlerImpl> =
    Resource.fromAutoCloseable {
        HandlerImpl(messagingConsumer, taskRepository)
    }