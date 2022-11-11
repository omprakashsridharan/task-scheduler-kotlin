package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import task.scheduler.kotlin.messaging.Messaging
import java.nio.charset.StandardCharsets
import java.util.*

private val logger = KotlinLogging.logger {}
interface Handler : AutoCloseable {
    suspend fun handleTask(taskType: String): Either<Throwable, Option<Task>>
}

class HandlerImpl(private val messagingConsumer: Messaging.Consumer, private val taskRepository: TaskRepository) :
    Handler {
    override suspend fun handleTask(taskType: String): Either<Throwable, Option<Task>> = either {
        val consumerTag = UUID.randomUUID()
        val taskString =
            String(messagingConsumer.consume(taskType, consumerTag.toString()).bind(), StandardCharsets.UTF_8)
        val task = Either.catch {
            Json.decodeFromString<Task>(taskString)
        }.bind()
        val isTaskValid = taskRepository.isTaskValid(task.taskId).bind()
        if (isTaskValid) {
            Some(task)
        } else {
            None
        }
    }

    override fun close() {
        logger.info { "Closing TaskHandler" }
    }
}

fun handler(messagingConsumer: Messaging.Consumer, taskRepository: TaskRepository): Resource<Handler> =
    Resource.fromAutoCloseable {
        HandlerImpl(messagingConsumer, taskRepository)
    }