package task.scheduler.kotlin.task

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable

@Serializable
data class TaskScheduleRequest(
    val taskType: String, val ttlInMilliSeconds: ULong,
    val payload: String
)

@Serializable
data class TaskHandleRequest(
    val taskType: String
)

fun Application.taskRoutes(
    taskScheduler: Scheduler,
    taskHandler: Handler
) = routing {
    route("/task") {
        post("/schedule") {
            val req = call.receive<TaskScheduleRequest>()
            val task =
                Task(taskType = req.taskType, ttlInMilliSeconds = (req.ttlInMilliSeconds), payload = req.payload)
            taskScheduler.scheduleTask(req.ttlInMilliSeconds, task).map {
                call.respond(HttpStatusCode.OK, it)
            }.mapLeft { call.respond(HttpStatusCode.InternalServerError) }

        }

        post("/handle") {
            val req = call.receive<TaskHandleRequest>()
            taskHandler.handleTask(req.taskType).map { taskOption ->
                taskOption.fold(
                    { call.respond(HttpStatusCode.OK, "TASK_INVALIDATED") },
                    { call.respond(HttpStatusCode.OK, it) })
            }.mapLeft { call.respond(HttpStatusCode.InternalServerError) }
        }
    }
}