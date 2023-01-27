package example

import java.util.UUID
import java.util.Properties
import scala.util.Random
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.jdk.CollectionConverters._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{ Encoder, Decoder }
import cats.syntax.all._
import cats.effect.{ IO, IOApp, Sync, Resource }
import fs2.Stream
import fs2.kafka._
import serdes.circe._
import org.apache.kafka.streams.processor.api._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.common.serialization.Serdes
import java.{util => ju}
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.Punctuator

object ProcessorApiExample {

  import domain._

  def totalPriceOrderProcessorSupplier(
    storeName: String
  ) = new ProcessorSupplier[String, ElectronicOrder, String, Double] {

    def get() = new Processor[String, ElectronicOrder, String, Double] {

      var store: KeyValueStore[String, Double] = _

      override def init(context: ProcessorContext[String, Double]): Unit = {
        store = context.getStateStore(storeName)
        context.schedule(
          10.seconds.toJava,
          PunctuationType.STREAM_TIME,
          forwardAll(context)
        )
      }

      def forwardAll(context: ProcessorContext[String, Double]): Punctuator =
        timestamp => store.all().asScala.foreach { p =>
          context.forward(new Record(
            p.key,
            p.value,
            timestamp
          ))  
        }

      def process(r: Record[String, ElectronicOrder]): Unit = {
        val key = r.key()
        val currentTotal = Option(store.get(key)).getOrElse(0.0)
        store.put(
          key,
          r.value().price + currentTotal
        )
      }
    }

    override def stores(): ju.Set[StoreBuilder[_ <: Object]] = 
      ju.Collections.singleton(totaPriceStoreBuilder)
  }

  val totaPriceStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore(config.storeName),
    Serdes.String(),
    Serdes.Double()
  )

  // Topology
  val electronicSerde = serde[ElectronicOrder]
  val uuidSerde = Serdes.UUID()
  val doubleSerde = Serdes.Double()

  val topology = new Topology

  topology.addSource(
    nodes.source,
    uuidSerde.deserializer(),
    electronicSerde.deserializer(),
    config.inputTopic
  )

  topology.addProcessor(
    nodes.aggregatePrice, // name of the node
    totalPriceOrderProcessorSupplier(config.storeName),
    nodes.source // Parent node
  )

  topology.addSink(
    nodes.sink,
    config.outputTopic, // output topic
    uuidSerde.serializer(),
    doubleSerde.serializer(),
    nodes.aggregatePrice // parent node
  )

  object nodes {

    val source = "source-node"
    val aggregatePrice = "aggregate-price"
    val sink = "sink-node"
  }

  object config {
  
    val applicationId = "processor-api-example"
    val bootstrapServers = "0.0.0.0:9092"
    val schemaRegistryUri = "http://0.0.0.0:8081"
    val inputTopic = "processor-input-topic"
    val outputTopic = "processor-output-topic"
    val storeName = "total-price-store"
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

    final case class OrderTotal(
      id: UUID,
      total: Double
    )

    object OrderTotal {
      implicit val jsonEncoder: Encoder[OrderTotal] =
        deriveEncoder

      implicit val jsonDecoder: Decoder[OrderTotal] =
        deriveDecoder

      implicit def serializer[F[_] : Sync]: Serializer[F, OrderTotal] =
        Serializer.lift { _.asJson.noSpaces.getBytes.pure }
    }
  }
}