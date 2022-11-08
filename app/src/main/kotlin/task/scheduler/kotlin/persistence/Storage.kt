package task.scheduler.kotlin.persistence

import arrow.core.Either
import arrow.core.Option

interface Storage : AutoCloseable {
    suspend fun set(key: String, value: String, timeToLiveInSeconds: Option<ULong>): Either<Throwable, Unit>
    suspend fun get(key: String): Either<Throwable, Option<String>>
    suspend fun delete(vararg keys: String): Either<Throwable, Long>
    suspend fun exists(key: String): Either<Throwable, Boolean>
}