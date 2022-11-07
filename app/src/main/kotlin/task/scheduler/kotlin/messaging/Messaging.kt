package task.scheduler.kotlin.messaging

import arrow.core.Either

object Messaging {
    interface Producer {
        suspend fun sendDelayedMessageToQueue(taskType:String, delayInMillis: Int, data: String): Either<Throwable,Unit>
    }

    interface Consumer {

    }
}
