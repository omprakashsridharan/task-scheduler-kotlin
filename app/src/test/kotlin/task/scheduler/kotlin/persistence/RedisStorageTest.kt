package task.scheduler.kotlin.persistence

import arrow.core.None
import arrow.core.Some
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import task.scheduler.kotlin.config.Env


val REDIS_IMAGE: DockerImageName = DockerImageName.parse("redis:7.0.5")


@Testcontainers
internal class RedisStorageTest {


    companion object {
        lateinit var redisContainer: GenericContainer<*>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            redisContainer = GenericContainer(REDIS_IMAGE)
                .withExposedPorts(6379).waitingFor(Wait.forListeningPort())
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
            redisContainer.start()
        }
    }

    @Test
    fun `set key without expiry`() {
        val redisStorage =
            RedisStorage(Env.Redis(redisContainer.host, redisContainer.firstMappedPort))
        runBlocking {
            redisStorage.set("KEY", "VALUE", None)
            val value = redisStorage.get("KEY")
            assertEquals(Some("VALUE"), value)
        }
    }
}