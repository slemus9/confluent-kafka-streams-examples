package example

import java.util.UUID
import java.util.Properties
import java.time.{ Instant, ZoneOffset }
import scala.collection.JavaConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration._
import io.circe.generic.semiauto._
import io.circe.{ Encoder, Decoder }
import cats.syntax.all._
import cats.effect.{ IO, IOApp, Sync, Resource}
import fs2.kafka.{ KafkaAdminClient, AdminClientSettings }
import serdes.circe._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology

object JoinsExample extends IOApp.Simple {

  import domain._
  
  val builder = new StreamsBuilder

  val applianceStream: KStream[UUID, ApplianceOrder] =
    builder.stream(config.applianceTopic)

  val electronicStream: KStream[UUID, ElectronicOrder] =
    builder.stream(config.electronicTopic)

  val userTable: KTable[UUID, User] =
    builder.table(
      config.userTopic,
      Materialized.as("user-store")
    )

  val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(
    30.minutes.toJava
  )

  val combinedStream: KStream[UUID, CombinedOrder] = 
    electronicStream
      .join(applianceStream)(
        CombinedOrder.fromOrder(_, _), 
        joinWindow
      )
      
  val combinedWithUserStream: KStream[UUID, UserCombinedOrder] =
    combinedStream
      .leftJoin(userTable)(
        UserCombinedOrder.apply
      )
      .peek { (_, order) => println(order) }

  

  def run: IO[Unit] = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)

    
    val simpleTopics = 
      List(
        config.applianceTopic,
        config.electronicTopic,
        config.combinedTopic,
        config.userCombinedTopic
      )
      .map { name => 
        new NewTopic(name, 1, 1.toShort)  
      }

    val makeTopology: IO[Topology] = IO {
      combinedWithUserStream.to(config.userCombinedTopic)
      builder.build()
    }.flatTap { topo => IO.println(topo.describe()) }

    val userTableTopic = 
      new NewTopic(
        config.userTopic,
        1,
        1.toShort
      )
      .configs(Map.from(List(
        TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT
      )).asJava)
    
    adminResource.use { admin => 
      admin
        .createTopics(userTableTopic :: simpleTopics)
        .flatMap { _ => makeTopology }
        .void
    }
  }

  val adminResource: Resource[IO, KafkaAdminClient[IO]] = 
    KafkaAdminClient.resource(
      AdminClientSettings(config.bootstrapServers)
    )
}

object config {

  val applicationId = "joins-example"
  val bootstrapServers = "0.0.0.0:9092"
  val schemaRegistryUri = "http://0.0.0.0:8081"
  val userTopic = "user-topic"
  val applianceTopic = "appliance-order-topic"
  val electronicTopic = "electronic-order-topic"
  val combinedTopic = "combined-order-topic"  
  val userCombinedTopic = "user-combined-order-topic"
}

object domain {

  final case class User(
    id: UUID,
    userName: String
  ) 
  
  object User {
    implicit val jsonEncoder: Encoder[User] = 
      deriveEncoder

    implicit val jsonDecoder: Decoder[User] =
      deriveDecoder
  }

  final case class ApplianceOrder(
    id: UUID,
    itemId: UUID,
    quantity: Int,
    date: Instant
  )

  object ApplianceOrder {
    implicit val jsonEncoder: Encoder[ApplianceOrder] = 
      deriveEncoder

    implicit val jsonDecoder: Decoder[ApplianceOrder] =
      deriveDecoder
  }

  final case class ElectronicOrder(
    id: UUID,
    itemId: UUID,
    quantity: Int,
    date: Instant
  )

  object ElectronicOrder {
    implicit val jsonEncoder: Encoder[ElectronicOrder] =
      deriveEncoder

    implicit val jsonDecoder: Decoder[ElectronicOrder] =
      deriveDecoder
  }

  final case class CombinedOrder(
    applianceOrderId: UUID,
    electronicOrderId: UUID,
    itemId: UUID,
    date: Instant
  )

  object CombinedOrder {

    implicit val jsonEncoder: Encoder[CombinedOrder] =
      deriveEncoder

    implicit val jsonDecoder: Decoder[CombinedOrder] =
      deriveDecoder

    def fromOrder(
      electronicOrder: ElectronicOrder,
      applianceOrder: ApplianceOrder
    ) = CombinedOrder(
      applianceOrder.id,
      electronicOrder.id,
      applianceOrder.itemId,
      Instant.now()
    )
  }

  final case class UserCombinedOrder(
    order: CombinedOrder,
    user: User
  ) 
  
  object UserCombinedOrder {

    implicit val jsonEncoder: Encoder[UserCombinedOrder] =
      deriveEncoder

    implicit val jsonDecoder: Decoder[UserCombinedOrder] =
      deriveDecoder    
  }
}