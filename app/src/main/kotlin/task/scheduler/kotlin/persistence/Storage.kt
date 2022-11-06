package task.scheduler.kotlin.persistence

import arrow.core.Either
import arrow.core.Option

interface Storage {
    suspend fun set(key: String, value: String, timeToLiveInSeconds: Option<ULong>)
    suspend fun get(key: String): Option<String>
}