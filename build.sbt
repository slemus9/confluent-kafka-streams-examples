import Dependencies._

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  Library.catsEffect,
  Library.kafkaClients,
  Library.kafkaStreams,
  Library.kafkaStreamsScala,
  Library.fs2Kafka
)