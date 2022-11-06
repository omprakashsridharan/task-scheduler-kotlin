package task.scheduler.kotlin.config

import java.lang.System.getenv


data class Env(val redis: Redis = Redis(), val rabbitMq: RabbitMq = RabbitMq(), val http: Http = Http()) {

    data class Redis(
        val host: String = getenv("REDIS_HOST") ?: "0.0.0.0",
        val port: Int = getenv("REDIS_PORT").toIntOrNull() ?: 5672
    )

    data class RabbitMq(
        val url: String = getenv("RABBIT_MQ_URL") ?: throw Exception("RABBIT_MQ_URL missing")
    )

    data class Http(
        val port: Int = getenv("PORT")?.toIntOrNull() ?: 3000
    )
}
