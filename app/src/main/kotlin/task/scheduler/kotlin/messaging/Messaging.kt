package task.scheduler.kotlin.messaging

import arrow.core.Either

object Messaging {
    interface Producer : AutoCloseable {
        suspend fun sendDelayedMessageToQueue(
            taskType: String,
            delayInMillis: ULong,
            data: ByteArray
        ): Either<Throwable, Unit>
    }

    interface Consumer : AutoCloseable {
        suspend fun consume(
            taskType: String
        ): Either<Throwable, ByteArray>
    }
}
