package example

import java.util.UUID
import java.util.Properties
import scala.util.Random
import scala.concurrent.duration._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{ Encoder, Decoder }
import cats.syntax.all._
import cats.effect.{ IO, IOApp, Sync, Resource }
import fs2.Stream
import fs2.kafka._
import serdes.circe._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException

object AggregationsExample extends IOApp.Simple {

  import domain._

  val builder = new StreamsBuilder

  val electronicStream: KStream[UUID, ElectronicOrder] =
    builder.stream(config.electronicTopic)

  val totalBoughtStream: KStream[UUID, Double] =
    electronicStream
      .groupByKey
      .aggregate(0.0) { (key, order, total) => 
        order.price + total
      }
      .toStream
      .peek { (key, total) => println(s"key: $key, total: $total") }

  val buildTopology: IO[Topology] = IO {
    totalBoughtStream.to(config.totalBoughtTopic)
    builder.build()
  }

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)

    val topics = 
      List(config.electronicTopic, config.totalBoughtTopic)
        .map { name => new NewTopic(name, 1, 1.toShort) }

    adminResource.use { admin =>
      admin
        .createTopics(topics)
        .recoverWith {
          _: TopicExistsException => IO.unit
        }
        .flatMap { _ => buildTopology }
        .flatTap { topo => IO.println(topo.describe()) }
        .flatTap { _ => populateStream(20, 5).compile.drain }
        .flatMap { topo => 
          KafkaStreamsApp.start[IO](topo, props, 2.seconds)
        }
    }
  }


  val adminResource: Resource[IO, KafkaAdminClient[IO]] = 
    KafkaAdminClient.resource(
      AdminClientSettings(config.bootstrapServers)
    ) 

  def populateStream(numOrders: Int, chunkSize: Int): Stream[IO, Unit] = {
    val settings = 
      ProducerSettings[IO, UUID, ElectronicOrder]
        .withBootstrapServers(config.bootstrapServers)

    def orders: Stream[IO, ElectronicOrder] = Stream(
      ElectronicOrder(
        UUID.randomUUID(),
        Random.between(1.0, 101.0)
      )
    ) ++ orders

    orders
      .take(numOrders)
      .map { o =>
        ProducerRecord(config.electronicTopic, o.id, o)  
      }
      .chunkN(chunkSize, allowFewer = true)
      .map(ProducerRecords.chunk(_))
      .through(KafkaProducer.pipe(settings))
      .debug()
      .void
  }

  object config {
  
    val applicationId = "aggregations-example"
    val bootstrapServers = "0.0.0.0:9092"
    val schemaRegistryUri = "http://0.0.0.0:8081"
    val electronicTopic = "electronic-order-topic"
    val totalBoughtTopic = "total-bought-topic"
  }

  object domain {

    final case class ElectronicOrder(
      id: UUID,
      price: Double
    )

    object ElectronicOrder {
      
      implicit val jsonEncoder: Encoder[ElectronicOrder] =
        deriveEncoder

      implicit val jsonDecoder: Decoder[ElectronicOrder] =
        deriveDecoder

      implicit def serializer[F[_] : Sync]: Serializer[F, ElectronicOrder] =
        Serializer.lift { _.asJson.noSpaces.getBytes.pure }
    }
  }
}