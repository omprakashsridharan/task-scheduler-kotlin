package task.scheduler.kotlin.persistence

import arrow.core.Either
import arrow.core.Option
import java.io.Serializable

interface Storage {
    suspend fun set(key: String, value: String, timeToLiveInSeconds: Option<ULong>): Either<Throwable, Unit>
    suspend fun get(key: String): Either<Throwable,Option<String>>
}