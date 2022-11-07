package task.scheduler.kotlin.messaging

import arrow.core.Either

object Messaging {
    interface Producer {
        suspend fun sendDelayedMessageToQueue(
            taskType: String,
            delayInMillis: Int,
            data: ByteArray
        ): Either<Throwable, Unit>
    }

    interface Consumer {
        suspend fun consume(
            taskType: String
        ): Either<Throwable, ByteArray>
    }
}
