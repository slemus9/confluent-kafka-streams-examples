import sbt._

object Dependencies {

  object Version {

    val catsEffect = "3.3.14"
    val kafka = "3.3.1"
    val fs2kafka = "3.0.0-M8"
  }

  object Library {

    val catsEffect = "org.typelevel" %% "cats-effect" % Version.catsEffect

    val kafkaClients = "org.apache.kafka" % "kafka-clients" % Version.kafka
    val kafkaStreams = "org.apache.kafka" % "kafka-streams" % Version.kafka

    val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % Version.fs2kafka
    val fs2KafkaVulcan = "com.github.fd4s" %% "fs2-kafka-vulcan" % Version.fs2kafka
  }
}