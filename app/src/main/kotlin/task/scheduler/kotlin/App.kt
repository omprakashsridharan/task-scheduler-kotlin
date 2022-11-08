package task.scheduler.kotlin

import arrow.continuations.SuspendApp
import arrow.fx.coroutines.continuations.resource
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import mu.KotlinLogging
import task.scheduler.kotlin.config.Env
import task.scheduler.kotlin.messaging.amqp
import task.scheduler.kotlin.messaging.messaging
import task.scheduler.kotlin.persistence.redis
import task.scheduler.kotlin.task.*

private val logger = KotlinLogging.logger {}

data class Dependencies(val taskScheduler: Scheduler, val taskHandler: Handler) : AutoCloseable {
    override fun close() {
        logger.info("Closing dependencies")
        taskHandler.close()
        taskScheduler.close()
    }
}

fun dependencies(env: Env) = resource {
    val redisStorage = redis(env.redis).bind()
    val amqpBase = amqp(env.rabbitMq).bind()
    val messaging = messaging(amqpBase).bind()
    val taskRepository = taskRepository(redisStorage).bind()
    val taskScheduler = scheduler(messaging.first, taskRepository).bind()
    val taskHandler = handler(messaging.second, taskRepository).bind()
    Dependencies(taskScheduler, taskHandler)
}

fun main() = SuspendApp {
    val env = Env()
    resource {
        val dependencies = dependencies(env).use { it ->
            delay(10000)
        }
    }.use { awaitCancellation() }
}