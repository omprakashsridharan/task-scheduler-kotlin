package task.scheduler.kotlin.persistence

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromCloseable
import io.github.crackthecodeabhi.kreds.args.SetOption
import io.github.crackthecodeabhi.kreds.connection.Endpoint
import io.github.crackthecodeabhi.kreds.connection.KredsClient
import io.github.crackthecodeabhi.kreds.connection.newClient
import task.scheduler.kotlin.config.Env
import java.io.Closeable

class RedisStorage(redisConfig: Env.Redis) : Closeable, Storage {
    private val redisClient: KredsClient = newClient(Endpoint(redisConfig.host, redisConfig.port))
    override fun close() {
        redisClient.close()
    }

    override suspend fun setWithExpiry(key: String, value: String, timeToLiveInSeconds: Option<ULong>) {
        redisClient.use { client ->
            when (timeToLiveInSeconds) {
                None -> client.set(key, value)
                is Some -> client.set(key, value, SetOption.Builder().exSeconds(timeToLiveInSeconds.value).build())
            }
        }
    }

}

fun redis(redisConfig: Env.Redis): Resource<RedisStorage> = Resource.fromCloseable {
    RedisStorage(redisConfig)
}