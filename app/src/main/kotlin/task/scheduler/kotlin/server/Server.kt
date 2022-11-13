package task.scheduler.kotlin.server

import arrow.fx.coroutines.Resource
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.defaultheaders.*
import io.ktor.server.plugins.requestvalidation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import task.scheduler.kotlin.Dependencies
import task.scheduler.kotlin.task.TaskScheduleRequest
import task.scheduler.kotlin.task.taskRoutes
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

fun <TEngine : ApplicationEngine, TConfiguration : ApplicationEngine.Configuration> server(
    factory: ApplicationEngineFactory<TEngine, TConfiguration>,
    port: Int = 80,
    host: String = "0.0.0.0",
    configure: TConfiguration.() -> Unit = {},
    preWait: Duration = 5.seconds,
    grace: Duration = 1.seconds,
    timeout: Duration = 5.seconds,
): Resource<ApplicationEngine> =
    Resource({
        embeddedServer(factory, host = host, port = port, configure = configure) {
        }.apply { start() }
    }, { engine, _ ->
        if (!engine.environment.developmentMode) {
            engine.environment.log.info(
                "prewait delay of ${preWait.inWholeMilliseconds}ms, turn it off using io.ktor.development=true"
            )
            delay(preWait.inWholeMilliseconds)
        }
        engine.environment.log.info("Shutting down HTTP server...")
        engine.stop(grace.inWholeMilliseconds, timeout.inWholeMilliseconds)
        engine.environment.log.info("HTTP server shutdown!")
    })

fun Application.configure(dependencies: Dependencies) {
    val appMicrometerRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    install(MicrometerMetrics) {
        registry = appMicrometerRegistry
        meterBinders = listOf(
            ClassLoaderMetrics(),
            JvmMemoryMetrics(),
            JvmGcMetrics(),
            ProcessorMetrics(),
            JvmThreadMetrics(),
            FileDescriptorMetrics(),
            UptimeMetrics()
        )
    }
    routing {
        get("/metrics") {
            call.respond(appMicrometerRegistry.scrape())
        }
    }
    install(DefaultHeaders)
    install(ContentNegotiation) {
        json(
            Json
        )
    }
    install(RequestValidation) {
        validate<TaskScheduleRequest> {
            ValidationResult.Valid
        }
    }
    taskRoutes(dependencies.taskScheduler, dependencies.taskHandler)
}
