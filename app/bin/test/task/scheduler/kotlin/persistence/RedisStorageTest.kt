package task.scheduler.kotlin.persistence

import arrow.core.None
import arrow.core.Some
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import task.scheduler.kotlin.config.Env


val REDIS_IMAGE: DockerImageName = DockerImageName.parse("redis:7.0.5")


@Testcontainers
internal class RedisStorageTest {

    lateinit var redisContainer: GenericContainer<*>

    @BeforeEach
    internal fun beforeEach(): Unit {
        redisContainer = GenericContainer(REDIS_IMAGE)
            .withExposedPorts(6379).waitingFor(Wait.forListeningPort())
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
        redisContainer.start()
    }

    @AfterEach
    internal fun afterEach(): Unit {
        redisContainer.stop()
    }

    @Test
    fun `set key without expiry`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            redisStorage.set("KEY", "VALUE", None)
            val getResult = redisStorage.get("KEY")
            getResult.map { assertEquals(Some("VALUE"), it) }
        }
    }

    @Test
    fun `set key with expiry`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            val setResult = redisStorage.set("KEY", "VALUE", Some(1u))
            assertTrue(setResult.isRight())
            val getResult1 = redisStorage.get("KEY")
            assertTrue(getResult1.isRight())
            getResult1.map { assertEquals(Some("VALUE"), it) }
            delay(1000)
            val getResult2 = redisStorage.get("KEY")
            getResult2.map { assertEquals(None, it) }

        }
    }

    @Test
    fun `set error handling`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            redisContainer.close()
            val setResult = redisStorage.set("KEY", "VALUE", Some(1u))
            assertTrue(setResult.isLeft())
        }
    }

    @Test
    fun `deletes two keys`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            val setResult1 = redisStorage.set("KEY1", "VALUE", None)
            assertTrue(setResult1.isRight())
            val setResult2 = redisStorage.set("KEY2", "VALUE", None)
            assertTrue(setResult2.isRight())
            val deleteResult = redisStorage.delete(*arrayOf("KEY1", "KEY2"))
            assertTrue(deleteResult.isRight())
            deleteResult.map { assertEquals(2, it) }
        }
    }

    @Test
    fun `test key exists`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            val setResult = redisStorage.set("KEY", "VALUE", None)
            assertTrue(setResult.isRight())
            val existResult = redisStorage.exists("KEY")
            assertTrue(existResult.isRight())
            existResult.map { assertTrue(it) }
        }
    }

    @Test
    fun `test key does not exists`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            val setResult = redisStorage.set("KEY", "VALUE", None)
            assertTrue(setResult.isRight())
            val existResult = redisStorage.exists("KEY1")
            assertTrue(existResult.isRight())
            existResult.map { assertFalse(it) }
        }
    }
}