/*
 * Copyright 2016 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.persistence.inmemory
package journal

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.{ Logging, LoggingAdapter }
import akka.pattern.ask
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, StorageExtension }
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.Timeout
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class InMemoryAsyncWriteJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val mat: Materializer = ActorMaterializer()
  val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val timeout: Timeout = Timeout(config.getDuration("ask-timeout", TimeUnit.SECONDS) -> SECONDS)
  val serialization = SerializationExtension(system)

  val journal: ActorRef = StorageExtension(system).journalStorage

  private def serialize(persistentRepr: PersistentRepr): Try[Array[Byte]] = persistentRepr.payload match {
    case Tagged(payload, _) => serialization.serialize(persistentRepr.withPayload(payload))
    case _                  => serialization.serialize(persistentRepr)
  }

  def toJournalEntries(w: AtomicWrite): Try[List[JournalEntry]] = {

    val allResults: Seq[Try[JournalEntry]] = w.payload.map { repr =>
      serialize(repr).map(bytes => JournalEntry(repr.persistenceId, repr.sequenceNr, bytes, repr.getTags))
    }

    allResults.collectFirst {
      case Failure(exception) => exception
    } match {
      case Some(exception) => Failure(exception)
      case None => Success(allResults.collect {
        case Success(seq) => seq
      }.toList)
    }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {

    // note; AtomicWrite guarantees that it is concerning the same persistence id

    def writeEntries(entries: List[JournalEntry]): Future[Try[Unit]] = (journal ? InMemoryJournalStorage.WriteEntries(entries)).map(_ => Success(()))

    val futures: Seq[Future[Try[Unit]]] = messages.map { write =>

      toJournalEntries(write) match {
        case Success(entries)   => writeEntries(entries)
        case Failure(exception) => Future.failed(exception)
      }
    }

    Future.sequence(futures)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    (journal ? InMemoryJournalStorage.DeleteEntries(persistenceId, toSequenceNr)).map(_ => ())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    (journal ? InMemoryJournalStorage.GetHighestSequenceNr(persistenceId, fromSequenceNr)).mapTo[Long]

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] =
    Source.fromFuture((journal ? InMemoryJournalStorage.GetEntries(persistenceId, fromSequenceNr, toSequenceNr, max, false)).mapTo[List[JournalEntry]])
      .mapConcat(identity)
      .via(deserialization)
      .runForeach(recoveryCallback)
      .map(_ => ())

  private val deserialization = Flow[JournalEntry].flatMapConcat { entry =>
    Source.fromFuture(Future.fromTry(serialization.deserialize(entry.serialized, classOf[PersistentRepr])))
      .map(_.update(deleted = entry.deleted))
  }
}
