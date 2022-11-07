package task.scheduler.kotlin.messaging

import arrow.core.Either
import arrow.core.continuations.either
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.continuations.resource
import arrow.fx.coroutines.fromAutoCloseable
import com.rabbitmq.client.*
import task.scheduler.kotlin.config.Env
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

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

    fun getChannel(): Either<Throwable, Channel> = Either.catch {
        connection.createChannel()
    }

    suspend fun assertExchange(exchange: String, type: BuiltinExchangeType): Either<Throwable, Unit> =
        either {
            val channel = getChannel().bind()
            channel.exchangeDeclare(exchange, type)
        }

    suspend fun assertQueue(queue: String, options: Map<String, Any>): Either<Throwable, Unit> = either {
        val channel = getChannel().bind()
        channel.queueDeclare(queue, true, false, false, options)
    }

    suspend fun bindQueue(
        queue: String,
        exchange: String,
        routingKey: String,
        arguments: Map<String, Any>
    ): Either<Throwable, Unit> = either {
        val channel = getChannel().bind()
        channel.queueBind(queue, exchange, routingKey, arguments)
    }

    suspend fun sendMessage(
        exchange: String,
        props: AMQP.BasicProperties,
        data: ByteArray
    ): Either<Throwable, Unit> =
        either {
            val channel = getChannel().bind()
            channel.basicPublish(exchange, "", props, data)
        }

    suspend fun consumeMessage(queue: String): ByteArray = suspendCoroutine { cont ->
        getChannel().map { channel: Channel ->
            val deliverCallback = DeliverCallback { _: String?, delivery: Delivery ->
                cont.resume(delivery.body)
            }
            val cancelCallback = CancelCallback { consumerTag: String? ->
                cont.resumeWithException(Exception("$consumerTag Cancelled"))
            }
            channel.basicConsume(queue, true, "consumer", deliverCallback, cancelCallback)
        }
    }
}

class AmqpProducer(rabbitMqConfig: Env.RabbitMq) : AmqpBase(rabbitMqConfig), AutoCloseable, Messaging.Producer {
    override fun close() {
        super.close()
    }

    override suspend fun sendDelayedMessageToQueue(
        taskType: String,
        delayInMillis: Int,
        data: ByteArray
    ): Either<Throwable, Unit> = either {
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

    override suspend fun consume(
        taskType: String
    ): Either<Throwable, ByteArray> = either {
        val finalQueue = "${taskType}_FINAL_QUEUE"
        val finalExchange = "${taskType}_FINAL_EXCHANGE"
        assertExchange(finalExchange, BuiltinExchangeType.FANOUT).bind()
        assertQueue(finalQueue, mapOf()).bind()
        bindQueue(finalQueue, finalExchange, "", mapOf()).bind()
        Either.catch {
            val message = consumeMessage(finalQueue)
            message
        }.bind()
    }
}

fun amqp(rabbitMqConfig: Env.RabbitMq): Resource<Pair<AmqpProducer, AmqpConsumer>> = resource {
    val producer = Resource.fromAutoCloseable { AmqpProducer(rabbitMqConfig) }.bind()
    val consumer = Resource.fromAutoCloseable { AmqpConsumer(rabbitMqConfig) }.bind()
    Pair(producer, consumer)
}