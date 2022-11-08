package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import task.scheduler.kotlin.messaging.Messaging

interface Scheduler : AutoCloseable {
    suspend fun scheduleTask(delayInMilliseconds: Int, task: Task): Either<Throwable, Unit>
}

private val logger = KotlinLogging.logger {}

class SchedulerImpl(private val messagingProducer: Messaging.Producer, private val taskRepository: TaskRepository) :
    Scheduler {
    override fun close() {
        logger.info { "Closing TaskScheduler" }
    }

    override suspend fun scheduleTask(delayInMilliseconds: Int, task: Task): Either<Throwable, Unit> = either {
        taskRepository
            .createTask(task.taskId, task.ttlInSeconds).bind()
        messagingProducer.sendDelayedMessageToQueue(
            task.taskType,
            delayInMilliseconds,
            Json.encodeToString(task).toByteArray()
        ).bind()
        task.taskId
    }
}

fun scheduler(messagingProducer: Messaging.Producer, taskRepository: TaskRepository): Resource<Scheduler> =
    Resource.fromAutoCloseable {
        SchedulerImpl(messagingProducer, taskRepository)
    }