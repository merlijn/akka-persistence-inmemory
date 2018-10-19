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
package extension

import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded }
import akka.event.LoggingReceive
import akka.persistence.PersistentRepr
import akka.persistence.inmemory.util.UUIDs
import akka.persistence.query.{ NoOffset, Offset, Sequence, TimeBasedUUID }
import akka.serialization.Serialization

import scala.collection.immutable._
import scalaz.syntax.semigroup._
import scalaz.std.AllInstances._

object InMemoryJournalStorage {

  sealed trait JournalCommand extends NoSerializationVerificationNeeded

  case object GetAllPersistenceIds extends JournalCommand

  final case class GetHighestSequenceNr(persistenceId: String, fromSequenceNr: Long) extends JournalCommand
  final case class GetEventsByTag(tag: String, offset: Offset) extends JournalCommand
  final case class WriteEntries(entries: Seq[JournalEntry]) extends JournalCommand
  final case class DeleteEntries(persistenceId: String, toSequenceNr: Long) extends JournalCommand
  final case class GetEntries(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, includeDeleted: Boolean) extends JournalCommand

  /**
   * Java API
   */
  def clearJournal(): ClearJournal = ClearJournal

  sealed abstract class ClearJournal
  case object ClearJournal extends ClearJournal with JournalCommand
}

class InMemoryJournalStorage(serialization: Serialization) extends Actor with ActorLogging {
  import InMemoryJournalStorage._

  var ordering: Long = 0L

  def incrementAndGet: Long = {
    ordering += 1
    ordering
  }

  var journal = Map.empty[String, Vector[JournalEntry]]

  def getAllPersistenceIds(): Set[String] = journal.keySet

  def getHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    journal.get(persistenceId)
      .map(_.map(_.sequenceNr).max)
      .getOrElse(0L)

  def getEventsByTag(tag: String, offset: Offset): List[JournalEntry] = {

    def increment(offset: Long): Long = offset + 1
    def getByOffset(p: JournalEntry => Boolean): List[JournalEntry] = {
      val xs = journal.values.flatten[JournalEntry].toVector
        .filter(_.tags.contains(tag)).toList
        .sortBy(_.ordering)
        .zipWithIndex.map {
          case (entry, index) =>
            entry.copy(offset = Option(increment(index)))
        }

      xs.filter(p)
    }

    offset match {
      case NoOffset             => getByOffset(_.offset.exists(_ >= 0L))
      case Sequence(value)      => getByOffset(_.offset.exists(_ > value))
      case value: TimeBasedUUID => getByOffset(p => UUIDs.TimeBasedUUIDOrdering.gt(p.timestamp, value))
    }
  }

  def writeEntries(entries: Seq[JournalEntry]): Unit = {

    val newEntries: Map[String, Seq[JournalEntry]] = entries.map(_.copy(ordering = incrementAndGet)).groupBy(_.persistenceId)
    journal = journal |+| newEntries
  }

  def deleteEntries(persistenceId: String, toSequenceNr: Long): Unit = {
    val pidEntries = journal.filter(_._1 == persistenceId)
    val notDeleted = pidEntries.mapValues(_.filterNot(_.sequenceNr <= toSequenceNr))

    val deleted = pidEntries
      .mapValues(_.filter(_.sequenceNr <= toSequenceNr).map { journalEntry =>
        val updatedRepr: PersistentRepr = journalEntry.repr.update(deleted = true)
        val byteArray: Array[Byte] = serialization.serialize(updatedRepr) match {
          case scala.util.Success(arr)   => arr
          case scala.util.Failure(cause) => throw cause
        }
        journalEntry.copy(deleted = true).copy(serialized = byteArray).copy(repr = updatedRepr)
      })

    journal = journal - persistenceId |+| deleted |+| notDeleted
  }

  def readEntries(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, includeDeleted: Boolean): List[JournalEntry] = {

    val entries: Iterator[JournalEntry] =
      journal.iterator
        .filter { case (key, _) => key == persistenceId }
        .flatMap { case (_, entries) => entries }
        .filter(_.sequenceNr >= fromSequenceNr)
        .filter(_.sequenceNr <= toSequenceNr)

    val entriesNotDeleted = if (includeDeleted) entries else entries.filterNot(_.deleted)

    val toTake = if (max >= Int.MaxValue) Int.MaxValue else max.toInt

    entriesNotDeleted.toList.sortBy(_.sequenceNr) take (toTake)
  }

  def clear(): Unit = {
    ordering = 0L
    journal = Map.empty[String, Vector[JournalEntry]]
  }

  import akka.actor.Status.Success

  override def receive: Receive = {
    case GetAllPersistenceIds                                                         => sender() ! Success(getAllPersistenceIds())
    case GetHighestSequenceNr(persistenceId, fromSequenceNr)                          => sender() ! Success(getHighestSequenceNr(persistenceId, fromSequenceNr))
    case GetEventsByTag(tag, offset)                                                  => sender() ! Success(getEventsByTag(tag, offset))
    case WriteEntries(entries)                                                        => sender() ! Success(writeEntries(entries))
    case DeleteEntries(persistenceId, toSequenceNr)                                   => sender() ! Success(deleteEntries(persistenceId, toSequenceNr))
    case GetEntries(persistenceId, fromSequenceNr, toSequenceNr, max, includeDeleted) => sender() ! Success(readEntries(persistenceId, fromSequenceNr, toSequenceNr, max, includeDeleted))
    case ClearJournal                                                                 => sender() ! Success(clear())
  }
}
