package example

import java.util.UUID
import java.time.{ Instant, ZoneOffset }
import cats.syntax.all._
import cats.effect.Sync
import vulcan.Codec
import fs2.kafka.{ Serializer, Deserializer }

object JoinsExample {

}

object domain {

  final case class User(
    userName: String
  ) 
  
  object User {
    implicit val avroCodec: Codec[User] = 
      Codec[String].imap { User(_) } { _.userName }
  }

  final case class ApplianceOrder(
    id: UUID,
    itemId: UUID,
    quantity: Int,
    date: Instant
  )

  object ApplianceOrder {
    implicit val avroCodec: Codec[ApplianceOrder] =
      Codec.record(
        name = "ApplianceOrder",
        namespace = "common.domain.event",
        doc = "An appliance order".some
      ) { field => 
        (
          field("id", _.id),
          field("itemId", _.itemId),
          field("quantity", _.quantity),
          field("date", _.date)
        ).mapN { ApplianceOrder.apply }
      }
  }

  final case class ElectronicOrder(
    id: UUID,
    itemId: UUID,
    quantity: Int,
    date: Instant
  )

  object ElectronicOrder {
    implicit val avroCodec: Codec[ElectronicOrder] =
      Codec.record(
        name = "ElectronicOrder",
        namespace = "common.domain.event",
        doc = "An electronic order".some
      ) { field => 
        (
          field("id", _.id),
          field("itemId", _.itemId),
          field("quantity", _.quantity),
          field("date", _.date)
        ).mapN { ElectronicOrder.apply }
      }
  }

  final case class CombinedOrder(
    applianceOrderId: UUID,
    electronicOrderId: UUID,
    itemId: UUID,
    date: Instant
  )

  object CombinedOrder {

    def fromOrder(
      applianceOrder: ApplianceOrder,
      electronicOrder: ElectronicOrder
    ) = CombinedOrder(
      applianceOrder.id,
      electronicOrder.id,
      applianceOrder.itemId,
      Instant.now()
    )

    implicit val avroCodec: Codec[CombinedOrder] =
      Codec.record(
        name = "CombinedOrder",
        namespace = "common.domain.event",
        doc = "A combination of an appliance and an electronic order".some
      ) { field =>
        (
          field("applianceOrderId", _.applianceOrderId),
          field("electronicOrderId", _.electronicOrderId),
          field("itemId", _.itemId),
          field("date", _.date)
        ).mapN { CombinedOrder.apply }
      }
  }
}