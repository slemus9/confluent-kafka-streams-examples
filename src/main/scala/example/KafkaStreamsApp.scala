package example

import java.util.Properties
import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import cats.syntax.all._
import cats.effect.{ Async, Resource, Deferred }
import org.apache.kafka.streams.{ KafkaStreams, Topology }
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import KafkaStreams.{State => KState}
import cats.effect.kernel.Outcome.Succeeded
import cats.effect.kernel.Outcome.Canceled
import java.lang.Thread.UncaughtExceptionHandler

object KafkaStreamsApp {

  def start[F[_]](
    topology: Topology, 
    properties: Properties,
    closeAfter: FiniteDuration,
    streamsUncaughtExceptionHandler: Option[StreamsUncaughtExceptionHandler]
  )(
    implicit F: Async[F]
  ) = 
    Resource
      .make(
        F.delay { new KafkaStreams(topology, properties) }
      )(app => 
        F.delay(app.close(closeAfter.toJava))  
      )
      .use(app => 
        F.async_[Unit] { cb => 

          app.setStateListener { 
            case (KState.ERROR, _) => cb(
              new RuntimeException("KafkaStreams application entered into an error state").asLeft
            )
            case (KState.NOT_RUNNING, _) =>
              cb(().asRight)
            case _ => ()
          }

          streamsUncaughtExceptionHandler match {
            case None => app.setUncaughtExceptionHandler { (_, e) => 
              cb(e.asLeft)
            }
            case Some(handler) => app.setUncaughtExceptionHandler(
              handler
            ) 
          }

          app.start()
        }  
      )
      
}