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

import akka.actor.{ Actor, ActorLogging, NoSerializationVerificationNeeded }
import akka.persistence.inmemory.util.UUIDs
import akka.persistence.query.{ NoOffset, Offset, Sequence, TimeBasedUUID }

import scala.collection.immutable._

object InMemoryJournalStorage {

  sealed trait JournalCommand extends NoSerializationVerificationNeeded

  case object GetAllPersistenceIds extends JournalCommand

  final case class GetHighestSequenceNr(persistenceId: String, fromSequenceNr: Long) extends JournalCommand
  final case class GetEventsByTag(tag: String, offset: Offset) extends JournalCommand
  final case class WriteEntries(entries: Seq[JournalEntry]) extends JournalCommand
  final case class DeleteEntries(persistenceId: String, toSequenceNr: Long) extends JournalCommand
  final case class GetEntries(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends JournalCommand

  /**
   * Java API
   */
  def clearJournal(): ClearJournal = ClearJournal

  sealed abstract class ClearJournal
  case object ClearJournal extends ClearJournal with JournalCommand
}

class InMemoryJournalStorage() extends Actor with ActorLogging {
  import InMemoryJournalStorage._

  var journal = Map.empty[String, Vector[JournalEntry]]

  def getAllPersistenceIds(): Set[String] = journal.keySet

  def getHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    journal.get(persistenceId)
      .map(_.map(_.sequenceNr).max)
      .getOrElse(0L)

  def getEventsByTag(tag: String, offset: Offset): List[JournalEntry] = {

    val entriesForTag = journal.values.flatten[JournalEntry].toVector
      .filter(_.tags.contains(tag)).toList
      .sortBy(_.timestamp)
      .zipWithIndex.map { case (entry, index) => entry.copy(offset = Option(index + 1)) }

    offset match {
      case NoOffset             => entriesForTag
      case Sequence(value)      => entriesForTag.drop(value.toInt)
      case value: TimeBasedUUID => entriesForTag.filter(p => UUIDs.TimeBasedUUIDOrdering.gt(p.timestamp, value))
    }
  }

  def writeEntries(entries: Seq[JournalEntry]): Unit = {

    val persistenceId = entries.head.persistenceId

    journal = journal + (persistenceId -> journal.getOrElse(persistenceId, Vector.empty).++(entries))
  }

  def deleteEntries(persistenceId: String, toSequenceNr: Long): Unit = {

    val newEntries: Vector[JournalEntry] =
      journal.getOrElse(persistenceId, Vector.empty).map { e =>
        if (e.sequenceNr <= toSequenceNr)
          e.copy(deleted = true)
        else
          e
      }

    journal = journal + (persistenceId -> newEntries)
  }

  def getEntries(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[JournalEntry] = {

    val entries: Iterator[JournalEntry] =
      journal.getOrElse(persistenceId, Vector.empty).iterator
        .filter(_.sequenceNr >= fromSequenceNr)
        .filter(_.sequenceNr <= toSequenceNr)
        .filterNot(_.deleted)

    val toTake: Long = Math.min(Int.MaxValue, max)

    entries.toList.sortBy(_.sequenceNr) take (toTake.toInt)
  }

  def clear(): Unit = {
    journal = Map.empty[String, Vector[JournalEntry]]
  }

  import akka.actor.Status.Success

  override def receive: Receive = {
    case GetAllPersistenceIds                                         => sender() ! Success(getAllPersistenceIds())
    case GetHighestSequenceNr(persistenceId, fromSequenceNr)          => sender() ! Success(getHighestSequenceNr(persistenceId, fromSequenceNr))
    case GetEventsByTag(tag, offset)                                  => sender() ! Success(getEventsByTag(tag, offset))
    case WriteEntries(entries)                                        => sender() ! Success(writeEntries(entries))
    case DeleteEntries(persistenceId, toSequenceNr)                   => sender() ! Success(deleteEntries(persistenceId, toSequenceNr))
    case GetEntries(persistenceId, fromSequenceNr, toSequenceNr, max) => sender() ! Success(getEntries(persistenceId, fromSequenceNr, toSequenceNr, max))
    case ClearJournal                                                 => sender() ! Success(clear())
  }
}
