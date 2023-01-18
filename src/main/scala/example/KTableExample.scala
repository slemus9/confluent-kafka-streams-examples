package example

import cats.syntax.all._
import cats.effect.{ IO, IOApp, Resource }
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.utils.Bytes
import java.util.Properties
import scala.util.Random
import scala.jdk.CollectionConverters._

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
    
    adminResource.use { admin => 
      setupTopic(admin, inputTopicName) >>
      setupTopic(admin, outputTopicName) >>
      populateStream.compile.drain >>
      IO {
        processStream(builder).to(
          outputTopicName,
          Produced.`with`(Serdes.String, Serdes.String)
        )
        new KafkaStreams(builder.build, props).start()
      }  
    }
  }

  def processStream(builder: StreamsBuilder): KStream[String, String] = {
    val ktable: KTable[String, String] = 
      builder.table(
        inputTopicName,
        Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("ktable-store")
          .withKeySerde(Serdes.String)
          .withValueSerde(Serdes.String)
      )

    ktable
      .filter { (k, v) => v.contains(targetSubstring) }
      .mapValues { v => v.substring(v.indexOf("-") + 1) }
      .filter { (k, v) => v.toLong > 1000 }
      .toStream()
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