package task.scheduler.kotlin.persistence

import arrow.core.Option
import io.github.crackthecodeabhi.kreds.connection.KredsClient

interface Storage {
    suspend fun setWithExpiry(key: String, value: String, timeToLiveInSeconds: Option<ULong>)
}