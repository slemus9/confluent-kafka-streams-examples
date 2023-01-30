package example

import java.util.Properties
import java.{util => ju}
import scala.util.Random
import scala.concurrent.duration._
import cats.syntax.all._
import cats.effect.{ IO, IOApp, Sync, Resource }
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import DeserializationExceptionHandler.DeserializationHandlerResponse
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import ProductionExceptionHandler.ProductionExceptionHandlerResponse
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.clients.admin.NewTopic

object ErrorHandlingExample extends IOApp.Simple {

  class DeserializationErrorHandler extends DeserializationExceptionHandler {

    var errorCount = 0

    def handle(
      context: ProcessorContext, 
      record: ConsumerRecord[Array[Byte], Array[Byte]], 
      exception: Exception
    ): DeserializationHandlerResponse = {
      errorCount = errorCount + 1
      if (errorCount < 25) DeserializationHandlerResponse.CONTINUE
      else DeserializationHandlerResponse.FAIL
    }

    def configure(configs: ju.Map[String, _ <: Object]): Unit = ()
  }

  class ProducerErrorHandler extends ProductionExceptionHandler {

    def handle(
      record: ProducerRecord[Array[Byte], Array[Byte]], 
      exception: Exception
    ): ProductionExceptionHandlerResponse = exception match {
      case _: RecordTooLargeException => 
        ProductionExceptionHandlerResponse.CONTINUE
      case _ =>
        ProductionExceptionHandlerResponse.FAIL
    }

    def configure(configs: ju.Map[String, _ <: Object]): Unit = ()
  }

  class CustomUncaughtExcHandler extends StreamsUncaughtExceptionHandler {

    def handle(exception: Throwable): StreamThreadExceptionResponse = 
      exception match {
        case e: StreamsException
          if e.getCause().getMessage() == config.userException =>
            StreamThreadExceptionResponse.REPLACE_THREAD
        case _ => 
            StreamThreadExceptionResponse.SHUTDOWN_CLIENT
      }
  }

  val builder = new StreamsBuilder

  val inStream: KStream[String, String] = 
    builder.stream(config.inputTopic)

  var throwException = true

  val buildTopology = IO {
    inStream
      .mapValues { 
        case config.invalidRecord if throwException => 
          throwException = false
          throw new IllegalStateException(config.userException)
        case s => s
      }
      .peek { (k, v) => s"Outgoing record - key: $k, value: $v" }
      .to(config.outputTopic)

    builder.build()
  }

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(
      StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      classOf[DeserializationErrorHandler]
    )
    props.put(
      StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
      classOf[ProducerErrorHandler]
    )
    
    val topics = 
      List(config.inputTopic, config.outputTopic)
        .map { name => new NewTopic(name, 1, 1.toShort) }

    adminResource.use { admin =>
      admin
        .createTopics(topics)
        .recoverWith {
          _: TopicExistsException => IO.unit
        }
        .flatMap { _ => buildTopology }
        .flatTap { topo => IO.println(topo.describe()) }
        .flatTap { _ => populateStream.compile.drain }
        .flatMap { topo => 
          KafkaStreamsApp.start[IO](
            topo, 
            props, 
            2.seconds, 
            Some(new CustomUncaughtExcHandler)
          )
        }
    }
  }

  val adminResource: Resource[IO, KafkaAdminClient[IO]] = 
    KafkaAdminClient.resource(
      AdminClientSettings(config.bootstrapServers)
    ) 

  def populateStream: Stream[IO, Unit] = {
    val settings = 
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)

    val values = Random.shuffle(
      config.invalidRecord :: List.fill(19) { Random.alphanumeric.take(4).mkString }
    )

    Stream.fromIterator[IO](values.iterator, values.size)
      .map { v =>
        fs2.kafka.ProducerRecord(config.inputTopic, v, v)  
      }
      .debug()
      .chunkAll
      .map(ProducerRecords.chunk(_))
      .through(KafkaProducer.pipe(settings))
      .void
  }

  object config {
  
    val applicationId = "error-handling-example"
    val bootstrapServers = "0.0.0.0:9092"
    val schemaRegistryUri = "http://0.0.0.0:8081"
    val inputTopic = "error-handling-input-topic"
    val outputTopic = "error-handling-output-topic"
    val userException = "Retryable transient error"
    val invalidRecord = "INVALID"
  }
}