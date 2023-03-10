package example

import cats.syntax.all._
import cats.effect.{ IO, IOApp, Resource }
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.streams.{ Topology, StreamsConfig, KafkaStreams }
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.common.config.TopicConfig
import java.util.Properties
import scala.util.Random
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object KTableExample extends IOApp.Simple {

  // Constants
  val applicationId = "ktable-example"
  val bootstrapServers = "0.0.0.0:9092"
  val inputTopicName = "ktable-example-in-topic"
  val outputTopicName = "ktable-example-out-topic"
  val targetSubstring = "orderNumber-"

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val builder = new StreamsBuilder
    processStream(builder).to(outputTopicName)

    adminResource.use { admin => 
      setupTopic(admin, inputTopicName) >>
      setupTopic(admin, outputTopicName) >>
      populateStream.compile.drain >>
      KafkaStreamsApp.start[IO](
        builder.build(),
        props,
        2.seconds,
        none
      )
    }
  }

  def processStream(builder: StreamsBuilder): KStream[String, String] = {
    val ktable: KTable[String, String] = 
      builder.table(
        inputTopicName,
        Materialized.as("ktable-store")
      )

    ktable
      .filter { (k, v) => v.contains(targetSubstring) }
      .mapValues { v => v.substring(v.indexOf("-") + 1) }
      .filter { (k, v) => v.toLong > 1000 }
      .toStream
      .peek { (k, v) => println(s"Outgoing Record. k: $k, v: $v") }
  }

  val adminResource: Resource[IO, KafkaAdminClient[IO]] = 
    KafkaAdminClient.resource(
      AdminClientSettings(bootstrapServers)
    )

  def setupTopic(
    admin: KafkaAdminClient[IO],
    topicName: String
  ): IO[Unit] = 
    admin
      .createTopic(
        new NewTopic(topicName, 1, 1.toShort)
          .configs(
            Map
              .from(List(
                TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT
              ))
              .asJava
          )
      )
      .recoverWith {
        // If the topic already exists, we just continue
        case _: TopicExistsException => IO.unit
      }

  /*
    Fill the input topic with random test data
  */
  def populateStream: Stream[IO, ProducerResult[Unit,String,String]] = {
    val settings = 
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServers)

    val ids = List.fill(6) { Random.between(980, 20000) }
    val records = ids.flatMap { id => 
      def newRecord = {
        val prefix = Random.alphanumeric.take(4).mkString
        val end = if (Random.nextDouble > 0.6) s"$targetSubstring$id" else s"-$id"
        ProducerRecord(inputTopicName, id.toString, prefix + end)  
      }

      List.fill(4) { newRecord }
    }

    Stream
      .fromIterator[IO](
        Random.shuffle(records).iterator, records.size
      )
      .chunkAll
      .map(ProducerRecords.chunk(_))
      .through(KafkaProducer.pipe(settings))
  }
}