package task.scheduler.kotlin.messaging

import arrow.core.Either
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.continuations.resource
import arrow.fx.coroutines.fromAutoCloseable
import com.rabbitmq.client.*
import task.scheduler.kotlin.config.Env
import java.nio.charset.StandardCharsets

open class AmqpBase(rabbitMqConfig: Env.RabbitMq) : AutoCloseable {
    private var connection: Connection

    init {
        val factory = ConnectionFactory()
        factory.setUri(rabbitMqConfig.url)
        connection = factory.newConnection()
    }

    override fun close() {
        connection.close()
    }

    private fun getChannel(): Either<Throwable, Channel> = Either.catch {
        connection.createChannel()
    }

    fun assertExchange(exchange: String, type: BuiltinExchangeType): Either<Throwable, Unit> = Either.catch {
        getChannel()
            .map { c ->
                c.exchangeDeclare(exchange, type)
            }
            .mapLeft { e -> throw e }
    }

    fun assertQueue(queue: String, options: Map<String, Any>): Either<Throwable, Unit> = Either.catch {
        getChannel()
            .map { c ->
                c.queueDeclare(queue, true, false, false, options)
            }
            .mapLeft { e -> throw e }
    }

    fun bindQueue(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Map<String, Any>
    ): Either<Throwable, Unit> = Either.catch {
        getChannel()
            .map { c ->
                c.queueBind(queue, exchange, routingKey, arguments)
            }
            .mapLeft { e -> throw e }
    }

    fun sendMessage(exchange: String, props: AMQP.BasicProperties, data: String): Either<Throwable, Unit> =
        Either.catch {
            getChannel().map { c ->
                c.basicPublish(exchange, "", props, data.toByteArray(StandardCharsets.UTF_8))
            }
                .mapLeft { e -> throw e }
        }

}

class AmqpProducer(rabbitMqConfig: Env.RabbitMq) : AmqpBase(rabbitMqConfig), AutoCloseable, Messaging.Producer {
    override fun close() {
        super.close()
    }

    override suspend fun sendDelayedMessageToQueue(
        taskType: String,
        delayInMillis: Int,
        data: String
    ): Either<Throwable, Unit> = either.eager {
        val intermediateQueue = "${taskType}_INTERMEDIATE_QUEUE"
        val intermediateExchange = "${taskType}_INTERMEDIATE_EXCHANGE"
        val finalQueue = "${taskType}_FINAL_QUEUE"
        val finalExchange = "${taskType}_FINAL_EXCHANGE"

        assertExchange(intermediateExchange, BuiltinExchangeType.FANOUT).bind()
        assertExchange(finalExchange, BuiltinExchangeType.FANOUT).bind()
        assertQueue(intermediateQueue, mapOf(("x-dead-letter-exchange" to finalExchange))).bind()
        assertQueue(finalQueue, mapOf()).bind()
        bindQueue(intermediateQueue, intermediateExchange, "", mapOf()).bind()
        bindQueue(finalQueue, finalExchange, "", mapOf()).bind()
        sendMessage(
            intermediateExchange,
            AMQP.BasicProperties.Builder().expiration(delayInMillis.toString()).build(),
            data
        ).bind()
    }
}

class AmqpConsumer(rabbitMqConfig: Env.RabbitMq) : AmqpBase(rabbitMqConfig), AutoCloseable, Messaging.Consumer {
    override fun close() {
        super.close()
    }
}

fun amqp(rabbitMqConfig: Env.RabbitMq): Resource<Pair<AmqpProducer, AmqpConsumer>> = resource {
    val producer = Resource.fromAutoCloseable { AmqpProducer(rabbitMqConfig) }.bind()
    val consumer = Resource.fromAutoCloseable { AmqpConsumer(rabbitMqConfig) }.bind()
    Pair(producer, consumer)
}