package task.scheduler.kotlin.messaging

import com.rabbitmq.client.AMQP
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
        runBlocking {
            val assertResult = amqpBase.assertQueue("TEST_QUEUE", mapOf())
            Assertions.assertTrue(assertResult.isRight())
        }
    }

    @Test
    fun bindQueue() {
        runBlocking {
            val exchangeAssertResult = amqpBase.assertExchange("TEST_EXCHANGE", BuiltinExchangeType.FANOUT)
            Assertions.assertTrue(exchangeAssertResult.isRight())
            val queueAssertResult = amqpBase.assertQueue("TEST_QUEUE", mapOf())
            Assertions.assertTrue(queueAssertResult.isRight())
            val bindResult = amqpBase.bindQueue("TEST_QUEUE", "TEST_EXCHANGE", "", mapOf())
            Assertions.assertTrue(bindResult.isRight())
        }
    }

    @Test
    fun sendMessage() {
        runBlocking {
            val exchangeAssertResult = amqpBase.assertExchange("TEST_EXCHANGE", BuiltinExchangeType.FANOUT)
            Assertions.assertTrue(exchangeAssertResult.isRight())
            val queueAssertResult = amqpBase.assertQueue("TEST_QUEUE", mapOf())
            Assertions.assertTrue(queueAssertResult.isRight())
            val bindResult = amqpBase.bindQueue("TEST_QUEUE", "TEST_EXCHANGE", "", mapOf())
            Assertions.assertTrue(bindResult.isRight())
            val sendMessageResult =
                amqpBase.sendMessage("TEST_EXCHANGE", AMQP.BasicProperties(), "MESSAGE".toByteArray())
            Assertions.assertTrue(sendMessageResult.isRight())
        }
    }

    @Test
    fun getChannel() {
        runBlocking {
            val assertResult = amqpBase.getChannel()
            Assertions.assertTrue(assertResult.isRight())
            assertResult.map { c ->
                Assertions.assertTrue(c.isOpen)
                c.close()
            }
        }
    }
}