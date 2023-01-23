package example.serdes

import io.circe.syntax._
import io.circe.parser.decode
import io.circe.{ Encoder, Decoder }
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes

object circe {

  implicit def serde[
    A >: Null : Encoder : Decoder
  ]: Serde[A] = {
    def serialize(a: A): Array[Byte] = a.asJson.noSpaces.getBytes
    def deserialize(bytes: Array[Byte]): Option[A] = {
      decode[A](new String(bytes)) match {
        case Left(e) => 
          println(e)
          None
        case Right(a) =>
          Some(a)
      }
    }

    Serdes.fromFn(serialize(_), deserialize)
  }
}