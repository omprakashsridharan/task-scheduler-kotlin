package task.scheduler.kotlin.persistence

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import arrow.fx.coroutines.fromCloseable
import io.github.crackthecodeabhi.kreds.args.SetOption
import io.github.crackthecodeabhi.kreds.connection.Endpoint
import io.github.crackthecodeabhi.kreds.connection.KredsClient
import io.github.crackthecodeabhi.kreds.connection.newClient
import task.scheduler.kotlin.config.Env

class RedisStorage(redisConfig: Env.Redis) : AutoCloseable, Storage {
    private val redisClient: KredsClient = newClient(Endpoint(redisConfig.host, redisConfig.port))
    override fun close() {
        redisClient.close()
    }

    override suspend fun set(key: String, value: String, timeToLiveInSeconds: Option<ULong>) {
        redisClient.use { client ->
            when (timeToLiveInSeconds) {
                None -> client.set(key, value)
                is Some -> client.set(key, value, SetOption.Builder().exSeconds(timeToLiveInSeconds.value).build())
            }
        }
    }

    override suspend fun get(key: String): Option<String> {
        return redisClient.use { client ->
            client.get(key)?.let {
                return@use Some(it)
            }
            return@use None
        }
    }
}

fun redis(redisConfig: Env.Redis): Resource<RedisStorage> = Resource.fromAutoCloseable {
    RedisStorage(redisConfig)
}