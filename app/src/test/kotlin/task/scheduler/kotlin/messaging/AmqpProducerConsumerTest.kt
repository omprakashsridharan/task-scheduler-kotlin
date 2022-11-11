package task.scheduler.kotlin.messaging

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import task.scheduler.kotlin.config.Env
import java.nio.charset.StandardCharsets

@Testcontainers
internal class AmqpProducerConsumerTest {
    private lateinit var rabbitMqContainer: RabbitMQContainer
    private lateinit var amqpProducer: AmqpProducer
    private lateinit var amqpConsumer: AmqpConsumer

    @BeforeEach
    internal fun beforeEach() {
        rabbitMqContainer =
            RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine"))
                .withExposedPorts(5672)
                .withVhost("test")
        rabbitMqContainer.start()
        val rabbitMqUrl = "amqp://guest:guest@localhost:${rabbitMqContainer.getMappedPort(5672)}/test"
        val amqpBase = AmqpBase(Env.RabbitMq(rabbitMqUrl))
        amqpProducer =
            AmqpProducer(amqpBase)
        amqpConsumer =
            AmqpConsumer(amqpBase)
    }

    @AfterEach
    internal fun afterEach() {
        rabbitMqContainer.stop()
    }

    @Test
    fun sendDelayedMessageToQueue() {
        runBlocking {
            val producerResult =
                amqpProducer.sendDelayedMessageToQueue("TEST_TASK", 2000u, "DATA".toByteArray(StandardCharsets.UTF_8))
            Assertions.assertTrue(producerResult.isRight())
            delay(2500)
            val consumeResult = amqpConsumer.consume("TEST_TASK", "TAG")
            Assertions.assertTrue(consumeResult.isRight())
            consumeResult.map {
                val receivedMessage = String(it, StandardCharsets.UTF_8)
                Assertions.assertEquals("DATA", receivedMessage)
            }
        }
    }
}