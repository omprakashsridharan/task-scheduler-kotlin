package task.scheduler.kotlin.config

import java.lang.System.getenv


data class Env(val redis: Redis = Redis(), val rabbitMq: RabbitMq = RabbitMq(), val http: Http = Http()) {

    data class Redis(
        val host: String = getenv("REDIS_HOST") ?: "0.0.0.0",
        val port: Int = getenv("REDIS_PORT")?.toIntOrNull() ?: 6379
    )

    data class RabbitMq(
        val url: String = getenv("RABBIT_MQ_URL") ?: "amqp://guest:guest@localhost:5672/orderserv"
    )

    data class Http(
        val host: String = getenv("HOST") ?: "0.0.0.0",
        val port: Int = getenv("PORT")?.toIntOrNull() ?: 8080
    )
}

