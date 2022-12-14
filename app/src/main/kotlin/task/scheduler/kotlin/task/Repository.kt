package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.Some
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import mu.KotlinLogging
import task.scheduler.kotlin.persistence.Storage

private val logger = KotlinLogging.logger {}
interface TaskRepository : AutoCloseable {
    suspend fun createTask(taskId: String, timeToLiveInMilliSeconds: ULong): Either<Throwable, Unit>
    suspend fun deleteTask(taskId: String): Either<Throwable, Long>
    suspend fun isTaskValid(taskId: String): Either<Throwable, Boolean>
}

class TaskRepositoryImpl(private val storage: Storage) : TaskRepository {
    override fun close() {
        logger.info { "Closing Task Repository" }
    }

    override suspend fun createTask(taskId: String, timeToLiveInMilliSeconds: ULong): Either<Throwable, Unit> = either {
        storage.set(taskId, "", Some(timeToLiveInMilliSeconds)).bind()
    }

    override suspend fun deleteTask(taskId: String): Either<Throwable, Long> = either {
        val x = storage.delete(taskId).bind()
        x
    }

    override suspend fun isTaskValid(taskId: String): Either<Throwable, Boolean> = either {
        storage.exists(taskId).bind()
    }
}

fun taskRepository(storage: Storage): Resource<TaskRepository> =
    Resource.fromAutoCloseable {
        TaskRepositoryImpl(storage)
    }