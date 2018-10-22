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

import akka.actor.{ Actor, ActorLogging }
import akka.persistence.query.Offset

import scala.collection.immutable._
import better.files._

class FlatFileJournalStorage extends Actor with ActorLogging {

  import InMemoryJournalStorage._

  val path = "/tmp/akka-journal"

  val journalDir = File(s"$path/journal").createIfNotExists(asDirectory = true)

  def getAllPersistenceIds(): Set[String] = {
    journalDir.list.map(_.name).toSet
  }

  def getHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = 0L

  def getEventsByTag(tag: String, offset: Offset): List[JournalEntry] = {

    List.empty
  }

  def writeEntries(entries: Seq[JournalEntry]): Unit = {
    entries.headOption match {
      case None => ()
      case Some(e) =>
        val persistenceId = e.persistenceId

        val index = (journalDir / s"$persistenceId.index").createFileIfNotExists()
        val journal = (journalDir / persistenceId).createFileIfNotExists()

        entries.foreach { entry =>
          index.appendLine(s"${entry.sequenceNr},${entry.serialized.length}")
          journal.appendByteArray(entry.serialized)
        }
    }
  }

  def deleteEntries(persistenceId: String, toSequenceNr: Long): Unit = {

  }

  def getEntries(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long): List[JournalEntry] = {

    List.empty
  }

  def clear(): Unit = {
    journalDir.delete()
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
