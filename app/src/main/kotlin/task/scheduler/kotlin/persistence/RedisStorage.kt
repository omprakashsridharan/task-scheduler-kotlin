package task.scheduler.kotlin.persistence

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.fromAutoCloseable
import io.github.crackthecodeabhi.kreds.connection.Endpoint
import io.github.crackthecodeabhi.kreds.connection.KredsClient
import io.github.crackthecodeabhi.kreds.connection.newClient
import task.scheduler.kotlin.config.Env

class RedisStorage(redisConfig: Env.Redis) : AutoCloseable, Storage {
    private val redisClient: KredsClient = newClient(Endpoint(redisConfig.host, redisConfig.port))
    override fun close() {
        redisClient.close()
    }

    override suspend fun set(key: String, value: String, timeToLiveInSeconds: Option<ULong>): Either<Throwable, Unit> =
        Either.catch {
            redisClient.use { client ->
                client.set(key, value)
                timeToLiveInSeconds.map {
                    val expirySet = client.expire(key, it)
                    if (expirySet.equals(0u)) {
                        throw Exception("Expiry was not set")
                    }
                }
            }
        }


    override suspend fun get(key: String): Either<Throwable, Option<String>> = Either.catch {
        redisClient.use { client ->
            client.get(key)?.let {
                return@use Some(it)
            }
            return@use None
        }
    }

    override suspend fun delete(vararg keys: String): Either<Throwable, Long> = Either.catch {
        redisClient.use { client ->
            return@use client.del(*keys)
        }
    }

    override suspend fun exists(key: String): Either<Throwable, Boolean> = Either.catch {
        redisClient.use { client ->
            client.exists(key) == 1L
        }
    }
}

fun redis(redisConfig: Env.Redis): Resource<Storage> = Resource.fromAutoCloseable {
    RedisStorage(redisConfig)
}