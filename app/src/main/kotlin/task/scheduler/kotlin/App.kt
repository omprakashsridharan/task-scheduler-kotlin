package task.scheduler.kotlin

import arrow.continuations.SuspendApp
import arrow.fx.coroutines.continuations.resource
import kotlinx.coroutines.awaitCancellation
import task.scheduler.kotlin.config.Env
import task.scheduler.kotlin.messaging.amqp
import task.scheduler.kotlin.persistence.redis
import task.scheduler.kotlin.task.*

data class Dependencies(val taskScheduler: Scheduler, val taskHandler: Handler)

suspend fun dependencies(env: Env) = resource {
    val redisStorage = redis(env.redis).bind()
    val messaging = amqp(env.rabbitMq).bind()
    val taskRepository = taskRepository(redisStorage).bind()
    val taskScheduler = scheduler(messaging.first, taskRepository).bind()
    val taskHandler = handler(messaging.second, taskRepository).bind()
    Dependencies(taskScheduler, taskHandler)
}

fun main() = SuspendApp {
    val env = Env()
    resource {
        dependencies(env)
    }.use { awaitCancellation() }
}