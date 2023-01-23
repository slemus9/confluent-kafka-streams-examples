import Dependencies._

ThisBuild / scalaVersion := "2.13.8"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  Library.catsEffect,
  Library.kafkaClients,
  Library.kafkaStreams,
  Library.kafkaStreamsScala,
  Library.fs2Kafka,
  Library.fs2KafkaVulcan
)