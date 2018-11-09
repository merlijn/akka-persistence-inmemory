package akka.persistence.flatfile

import akka.persistence.PersistentRepr
import akka.persistence.inmemory.protobuf
import akka.serialization.{Serialization, Serializer, SerializerWithStringManifest}
import com.google.protobuf.ByteString

import scala.util.{Failure, Try}

object FileJournalSerialization {

  def toProto(e: PersistentRepr, serialization: Serialization): Array[Byte] = {
    val entry = protobuf.JournalEntry(e.manifest, e.writerUuid, Some(toProtoPayload(e.payload.asInstanceOf[AnyRef], serialization)))
    protobuf.JournalEntry.messageCompanion.toByteArray(entry)
  }

  def toProtoPayload(payload: AnyRef, serialization: Serialization): protobuf.Payload = {

    val serializer: Serializer = serialization.findSerializerFor(payload)

    val bytes = serializer.toBinary(payload)

    val manifest = serializer match {
      case s: SerializerWithStringManifest ⇒ s.manifest(payload)
      case _                               ⇒ if (payload != null) payload.getClass.getName else ""
    }

    // we should not have to copy the bytes
    protobuf.Payload(
      serializerId = serializer.identifier,
      manifest = manifest,
      data = ByteString.copyFrom(bytes)
    )
  }

  def toDomain(bytes: Array[Byte], serialization: Serialization, persistenceId: String, sequenceNr: Long): Try[PersistentRepr] = {
    protobuf.JournalEntry.messageCompanion.parseFrom(bytes) match {

      case protobuf.JournalEntry(manifest, writeUUId, Some(payload)) =>
        serialization.deserialize(payload.data.toByteArray, payload.serializerId, payload.manifest).map { deserialized =>
          PersistentRepr(deserialized, sequenceNr, persistenceId, manifest, false, null, writeUUId)
        }
      case _ =>
        Failure(new RuntimeException("missing data"))
    }
  }
}
