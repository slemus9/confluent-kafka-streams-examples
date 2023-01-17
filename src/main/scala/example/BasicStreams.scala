package example

import cats.syntax.all._
import cats.effect.{ IO, IOApp, Resource }
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.common.serialization.Serdes
import java.util.Properties
import scala.util.Random

object BasicStreams extends IOApp.Simple {

  // Constants
  val applicationId = "basic-streams"
  val bootstrapServers = "0.0.0.0:9092"
  val inputTopicName = "basic-streams-in-topic"
  val outputTopicName = "basic-streams-out-topic"
  val targetSubstring = "orderNumber-"

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val process = IO {
      val builder = new StreamsBuilder
      processStream(builder).to(
        outputTopicName,
        Produced.`with`(Serdes.String, Serdes.String)
      )
      builder
    } 

    val populate = populateStream(chunkSize = 5).take(25).compile.drain

    adminResource.use { admin => 
      val createTopics = 
        setupTopic(admin, inputTopicName) >> setupTopic(admin, outputTopicName)

      createTopics >> populate >> process.flatMap(builder => IO {
        val streams = new KafkaStreams(builder.build, props) 
        streams.start()
      })
    }
  }


  def processStream(builder: StreamsBuilder): KStream[String, String] = {
    val inStream = builder.stream(
      inputTopicName, 
      Consumed.`with`(Serdes.String, Serdes.String)
    )

    inStream
      .peek { (k, v) => println(s"Before processing. key: $k . value: $v") }
      .filter { (_, v) => v.contains(targetSubstring) }
      .mapValues { v => v.substring(v.indexOf("-") + 1) }
      .filter { (_, v) => v.toLong > 1000 }
      .peek { (k, v) => println(s"After processing. key: $k . value: $v") }
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
      .createTopic(new NewTopic(
        topicName,
        1,
        1.toShort
      ))
      .recoverWith {
        // If the topic already exists, we just continue
        case _: TopicExistsException => IO.unit
      }

  /*
    Fill the input topic with random test data
  */
  def populateStream(
    chunkSize: Int
  ): Stream[IO, ProducerResult[Unit,String,String]] = {
    val settings = 
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServers)

    def randomIntStream: Stream[IO, Int] = 
      Stream(Random.between(0, 100000)) ++ randomIntStream

    randomIntStream
      .map { i => 
        val prefix = Random.alphanumeric.take(4).mkString
        val end = if (Random.nextDouble > 0.6) s"$targetSubstring$i" else ""
        val key = s"$prefix--$i"
        val value = prefix + end

        ProducerRecord(inputTopicName, key, value)
      }
      .chunkN(chunkSize, allowFewer = true)
      .map(ProducerRecords.chunk(_))
      .through(KafkaProducer.pipe(settings))
  }
}