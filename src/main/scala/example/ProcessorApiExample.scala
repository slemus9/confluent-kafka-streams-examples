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
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException

object ProcessorApiExample extends IOApp.Simple {

  import domain._

  def totalPriceOrderProcessorSupplier(
    storeName: String
  ) = new ProcessorSupplier[UUID, ElectronicOrder, UUID, Double] {

    def get() = new Processor[UUID, ElectronicOrder, UUID, Double] {

      var store: KeyValueStore[UUID, Double] = _

      override def init(context: ProcessorContext[UUID, Double]): Unit = {
        store = context.getStateStore(storeName)
        context.schedule(
          10.seconds.toJava,
          PunctuationType.STREAM_TIME,
          forwardAll(context)
        )
      }

      def forwardAll(context: ProcessorContext[UUID, Double]): Punctuator =
        timestamp => store.all().asScala.foreach { p =>
          context.forward(new Record(
            p.key,
            p.value,
            timestamp
          ))  
        }

      def process(r: Record[UUID, ElectronicOrder]): Unit = {
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
    Serdes.UUID(),
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

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)

    val topics = 
      List(config.inputTopic, config.outputTopic)
        .map { name => new NewTopic(name, 1, 1.toShort) }

    adminResource.use { admin =>
      admin
        .createTopics(topics)
        .recoverWith {
          _: TopicExistsException => IO.unit
        }
        .flatMap { _ => IO.println(topology.describe()) }
        .flatMap { _ => populateStream.compile.drain }
        .flatMap { _ => 
          KafkaStreamsApp.start[IO](topology, props, 2.seconds, none)
        }
    } 
  }

  val adminResource: Resource[IO, KafkaAdminClient[IO]] = 
    KafkaAdminClient.resource(
      AdminClientSettings(config.bootstrapServers)
    ) 

  def populateStream: Stream[IO, Unit] = {
    val settings = 
      ProducerSettings[IO, UUID, ElectronicOrder]
        .withBootstrapServers(config.bootstrapServers)

    val orderIds: List[UUID] = List.fill(5) { UUID.randomUUID }

    def makeOrder(id: UUID) = ElectronicOrder(
      id, Random.between(1.0, 101.0)
    )

    def orders: List[ElectronicOrder] = 
      Random.shuffle(
        orderIds.flatMap { id => List.fill(4) { makeOrder(id) } }
      )

    Stream.fromIterator[IO](orders.iterator, orders.size)
      .map { o =>
        ProducerRecord(config.inputTopic, o.id, o)  
      }
      .debug()
      .chunkAll
      .map(ProducerRecords.chunk(_))
      .through(KafkaProducer.pipe(settings))
      .void
  }

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
  }
}