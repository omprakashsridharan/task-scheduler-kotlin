package task.scheduler.kotlin.messaging

import com.rabbitmq.client.BuiltinExchangeType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import task.scheduler.kotlin.config.Env

@Testcontainers
internal class AmqpBaseTest {
    private lateinit var rabbitMqContainer: RabbitMQContainer
    private lateinit var amqpBase: AmqpBase

    @BeforeEach
    internal fun beforeEach() {
        rabbitMqContainer =
            RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine"))
                .withExposedPorts(5672)
                .withVhost("test")
        rabbitMqContainer.start()
        amqpBase =
            AmqpBase(Env.RabbitMq("amqp://guest:guest@localhost:${rabbitMqContainer.getMappedPort(5672)}/test"))
    }

    @AfterEach
    internal fun afterEach() {
        rabbitMqContainer.stop()
    }

    @Test
    fun assertExchange() {
        runBlocking {
            val assertResult = amqpBase.assertExchange("TEST", BuiltinExchangeType.FANOUT)
            Assertions.assertTrue(assertResult.isRight())
        }
    }

    @Test
    fun assertQueue() {
    }

    @Test
    fun bindQueue() {
    }

    @Test
    fun sendMessage() {
    }
}