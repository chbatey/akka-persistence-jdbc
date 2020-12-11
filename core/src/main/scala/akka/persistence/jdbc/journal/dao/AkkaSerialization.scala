package akka.persistence.jdbc.journal.dao

import akka.persistence.PersistentRepr
import akka.persistence.jdbc.journal.dao.JournalTables.JournalAkkaSerializationRow
import akka.serialization.{ Serialization, Serializers }

import scala.util.Try

object AkkaSerialization {

  case class AkkaSerialized(serId: Int, serManifest: String, payload: Array[Byte])

  def serialize(serialization: Serialization, payload: Any): Try[AkkaSerialized] = {
    val p2 = payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(p2)
    val serManifest = Serializers.manifestFor(serializer, p2)
    val serialized = serialization.serialize(p2)
    serialized.map(payload => AkkaSerialized(serializer.identifier, serManifest, payload))
  }

  def fromRow(serialization: Serialization)(row: JournalAkkaSerializationRow): Try[(PersistentRepr, Long)] = {
    serialization.deserialize(row.eventPayload, row.eventSerId, row.eventSerManifest).map { payload =>
      // TODO support metadata
      val repr = PersistentRepr(
        payload,
        row.sequenceNumber,
        row.persistenceId,
        row.eventManifest,
        row.deleted,
        sender = null,
        writerUuid = row.writer)

      (repr.withTimestamp(row.writeTimestamp), row.ordering)
    }
  }
}
