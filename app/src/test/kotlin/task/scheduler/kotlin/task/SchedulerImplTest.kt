package task.scheduler.kotlin.task

import arrow.core.Either
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import task.scheduler.kotlin.messaging.AmqpProducer

@ExtendWith(MockKExtension::class)
internal class SchedulerImplTest {
    private var taskRepositoryImpl = mockk<TaskRepositoryImpl>()
    private var messageProducer = mockk<AmqpProducer>()
    private var schedulerImpl = SchedulerImpl(messageProducer, taskRepositoryImpl)

    @Test
    fun `scheduleTask success`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val successCreateTaskRepositoryResult: Either<Throwable, Unit> = Either.catch { }
        val successSendDelayedMessageToQueueResult: Either<Throwable, Unit> = Either.catch { }
        runBlocking {
            coEvery {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
            } returns successCreateTaskRepositoryResult
            coEvery {
                messageProducer.sendDelayedMessageToQueue(
                    task.taskType,
                    1,
                    Json.encodeToString(task).toByteArray()
                )
            } returns successSendDelayedMessageToQueueResult
            val scheduleTaskResult = schedulerImpl.scheduleTask(1, task)
            coVerifyOrder {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
                messageProducer.sendDelayedMessageToQueue(
                    task.taskType,
                    1,
                    Json.encodeToString(task).toByteArray()
                )
            }
            assertTrue(scheduleTaskResult.isRight())
            scheduleTaskResult.map {
                assertEquals(task.taskId, it)
            }
        }
    }

    @Test
    fun `scheduleTask when taskRepository create fails`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val failureCreateTaskRepositoryResult: Either<Throwable, Unit> =
            Either.catch { throw Exception("TASK_CREATION_FAILED") }
        runBlocking {
            coEvery {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
            } returns failureCreateTaskRepositoryResult
            val scheduleTaskResult = schedulerImpl.scheduleTask(1, task)
            coVerify {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
            }
            assertTrue(scheduleTaskResult.isLeft())
            scheduleTaskResult.mapLeft {
                assertEquals("TASK_CREATION_FAILED", it.message)
            }
        }
    }

    @Test
    fun `scheduleTask when producer sendDelayedMessageToQueue fails`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val successCreateTaskRepositoryResult: Either<Throwable, Unit> = Either.catch { }
        val failureSendDelayedMessageToQueueResult: Either<Throwable, Unit> =
            Either.catch { throw Exception("SEND_TO_QUEUE_FAILED") }
        runBlocking {
            coEvery {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
            } returns successCreateTaskRepositoryResult
            coEvery {
                messageProducer.sendDelayedMessageToQueue(
                    task.taskType,
                    1,
                    Json.encodeToString(task).toByteArray()
                )
            } returns failureSendDelayedMessageToQueueResult
            val scheduleTaskResult = schedulerImpl.scheduleTask(1, task)
            coVerifyOrder {
                taskRepositoryImpl
                    .createTask(task.taskId, task.ttlInSeconds)
                messageProducer.sendDelayedMessageToQueue(
                    task.taskType,
                    1,
                    Json.encodeToString(task).toByteArray()
                )
            }
            assertTrue(scheduleTaskResult.isLeft())
            scheduleTaskResult.mapLeft {
                assertEquals("SEND_TO_QUEUE_FAILED", it.message)
            }
        }
    }
}