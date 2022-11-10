package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.None
import arrow.core.Some
import io.mockk.coEvery
import io.mockk.coVerifyOrder
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import task.scheduler.kotlin.messaging.AmqpConsumer

@ExtendWith(MockKExtension::class)
internal class HandlerImplTest {
    private var taskRepositoryImpl = mockk<TaskRepositoryImpl>()
    private var messageConsumer = mockk<AmqpConsumer>()
    private var handlerImpl = HandlerImpl(messageConsumer, taskRepositoryImpl)

    @Test
    fun `handleTask success valid task`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val successMessageConsumeResult: Either<Throwable, ByteArray> =
            Either.catch { Json.encodeToString(task).toByteArray() }
        val successTaskValidResult: Either<Throwable, Boolean> =
            Either.catch { true }
        runBlocking {
            coEvery {
                messageConsumer.consume(taskType = task.taskType)
            } returns successMessageConsumeResult

            coEvery {
                taskRepositoryImpl.isTaskValid(taskId = task.taskId)
            } returns successTaskValidResult

            val handleTaskResult = handlerImpl.handleTask(task.taskType)
            coVerifyOrder {
                messageConsumer.consume(task.taskType)
                taskRepositoryImpl.isTaskValid(taskId = task.taskId)
            }
            assertTrue(handleTaskResult.isRight())
            handleTaskResult.map {
                assertEquals(Some(task), it)
            }
        }
    }

    @Test
    fun `handleTask success invalid task`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val successMessageConsumeResult: Either<Throwable, ByteArray> =
            Either.catch { Json.encodeToString(task).toByteArray() }
        val successTaskValidResult: Either<Throwable, Boolean> =
            Either.catch { false }
        runBlocking {
            coEvery {
                messageConsumer.consume(taskType = task.taskType)
            } returns successMessageConsumeResult

            coEvery {
                taskRepositoryImpl.isTaskValid(taskId = task.taskId)
            } returns successTaskValidResult

            val handleTaskResult = handlerImpl.handleTask(task.taskType)
            coVerifyOrder {
                messageConsumer.consume(task.taskType)
                taskRepositoryImpl.isTaskValid(taskId = task.taskId)
            }
            assertTrue(handleTaskResult.isRight())
            handleTaskResult.map {
                assertEquals(None, it)
            }
        }
    }

    @Test
    fun `handleTask messageConsumer consume fails`() {
        val task = Task(taskType = "TYPE", ttlInSeconds = 1U, payload = "DATA")
        val failureMessageConsumeResult: Either<Throwable, ByteArray> =
            Either.catch { throw Exception("MESSAGE_CONSUME_FAILED") }
        runBlocking {
            coEvery {
                messageConsumer.consume(taskType = task.taskType)
            } returns failureMessageConsumeResult

            val handleTaskResult = handlerImpl.handleTask(task.taskType)
            coVerifyOrder {
                messageConsumer.consume(task.taskType)
            }
            assertTrue(handleTaskResult.isLeft())
            handleTaskResult.mapLeft {
                assertEquals("MESSAGE_CONSUME_FAILED", it.message)
            }
        }
    }
}