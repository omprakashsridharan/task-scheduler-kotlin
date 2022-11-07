package task.scheduler.kotlin

import arrow.continuations.SuspendApp
import arrow.fx.coroutines.continuations.resource
import kotlinx.coroutines.awaitCancellation
import task.scheduler.kotlin.config.Env
import task.scheduler.kotlin.messaging.Messaging
import task.scheduler.kotlin.messaging.amqp
import task.scheduler.kotlin.persistence.Storage
import task.scheduler.kotlin.persistence.redis

data class Dependencies(val storage: Storage, val messaging: Pair<Messaging.Producer,Messaging.Consumer>)


suspend fun dependencies(env: Env) = resource {
    val redisStorage = redis(env.redis).bind()
    val messaging = amqp(env.rabbitMq).bind()
    Dependencies(storage = redisStorage,messaging)
}

fun main() = SuspendApp {
    val env = Env()
    resource {
        val dependencies = dependencies(env).bind()
    }.use { awaitCancellation() }
}