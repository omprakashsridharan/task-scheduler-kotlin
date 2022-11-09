package task.scheduler.kotlin.task

import arrow.core.Either
import arrow.core.Some
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import task.scheduler.kotlin.persistence.RedisStorage

@ExtendWith(MockKExtension::class)
internal class TaskRepositoryImplTest {

    private var storage = mockk<RedisStorage>()
    private var taskRepositoryImpl: TaskRepositoryImpl = TaskRepositoryImpl(storage)

    @Test
    fun `createTask successful`() {
        runBlocking {
            val successRedisSet: Either<Throwable, Unit> = Either.catch { }
            coEvery { storage.set("TASK_ID", "", Some(1U)) } returns successRedisSet
            val result = taskRepositoryImpl.createTask("TASK_ID", 1U)
            coVerify {
                storage.set("TASK_ID", "", Some(1U))
            }
            assertTrue(result.isRight())
        }
    }

    @Test
    fun `createTask failure`() {
        runBlocking {
            val failureRedisSet: Either<Throwable, Unit> = Either.catch { throw Exception("CANNOT SET KEY") }
            coEvery { storage.set("TASK_ID", "", Some(1U)) } returns failureRedisSet
            val result = taskRepositoryImpl.createTask("TASK_ID", 1U)
            coVerify {
                storage.set("TASK_ID", "", Some(1U))
            }
            assertTrue(result.isLeft())
        }
    }

    @Test
    fun `deleteTask successful`() {
        runBlocking {
            val successRedisDelete: Either<Throwable, Long> = Either.catch { 1L }
            coEvery { storage.delete("TASK_ID") } returns successRedisDelete
            val result = taskRepositoryImpl.deleteTask("TASK_ID")
            coVerify {
                storage.delete("TASK_ID")
            }
            assertTrue(result.isRight())
            result.map {
                assertEquals(1L, it)
            }
        }
    }

    @Test
    fun `deleteTask failure`() {
        runBlocking {
            val failureRedisDelete: Either<Throwable, Long> = Either.catch { throw Exception("CANNOT DELETE KEY") }
            coEvery { storage.delete("TASK_ID") } returns failureRedisDelete
            val result = taskRepositoryImpl.deleteTask("TASK_ID")
            coVerify {
                storage.delete("TASK_ID")
            }
            assertTrue(result.isLeft())
        }
    }

    @Test
    fun `isTaskValid successful true`() {
        runBlocking {
            val successRedisDelete: Either<Throwable, Boolean> = Either.catch { true }
            coEvery { storage.exists("TASK_ID") } returns successRedisDelete
            val result = taskRepositoryImpl.isTaskValid("TASK_ID")
            coVerify {
                storage.exists("TASK_ID")
            }
            assertTrue(result.isRight())
            result.map {
                assertTrue(it)
            }
        }
    }

    @Test
    fun `isTaskValid successful false`() {
        runBlocking {
            val successRedisDelete: Either<Throwable, Boolean> = Either.catch { false }
            coEvery { storage.exists("TASK_ID") } returns successRedisDelete
            val result = taskRepositoryImpl.isTaskValid("TASK_ID")
            coVerify {
                storage.exists("TASK_ID")
            }
            assertTrue(result.isRight())
            result.map {
                assertFalse(it)
            }
        }
    }

    @Test
    fun `isTaskValid failure`() {
        runBlocking {
            val successRedisDelete: Either<Throwable, Boolean> = Either.catch { throw Exception("EXISTS exception") }
            coEvery { storage.exists("TASK_ID") } returns successRedisDelete
            val result = taskRepositoryImpl.isTaskValid("TASK_ID")
            coVerify {
                storage.exists("TASK_ID")
            }
            assertTrue(result.isLeft())
        }
    }
}