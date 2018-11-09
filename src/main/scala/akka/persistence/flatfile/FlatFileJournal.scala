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

package akka.persistence.flatfile

import java.nio.file.{Path, Paths}

import akka.persistence.flatfile.FileUtil._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import better.files._
import com.typesafe.config.Config

import scala.collection.immutable._
import scala.concurrent.Future
import scala.util.Try

class FlatFileJournal(config: Config) extends AsyncWriteJournal {

  val path = config.getString("path")
  val journalDir = File(path).createIfNotExists(asDirectory = true)

  val serialization: Serialization = SerializationExtension(context.system)

  val indexSize = 12

  val sequenceNrMaxMessage = "Only sequence numbers up to Int.MaxValue are supported"

  import context.dispatcher

  def indexFile(persistenceId: String): Path = Paths.get(s"$path/$persistenceId.index").createFileIfNotExists()
  def storeFile(persistenceId: String): Path = Paths.get(s"$path/$persistenceId.store").createFileIfNotExists()

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = Future {

    messages.sortBy(_.lowestSequenceNr).map { write =>

      Try {

        val seqs = write.payload.map(_.sequenceNr).mkString(",")

        val index = indexFile(write.persistenceId)
        val store = storeFile(write.persistenceId)

        val lastWrittenSeq = (index.size() / 12)

        if (write.lowestSequenceNr != lastWrittenSeq + 1)
          throw new IllegalArgumentException(s"Expected next sequence nr: ${lastWrittenSeq + 1}, got: ${write.lowestSequenceNr}")

//        println(s"write : ${write.persistenceId} : $seqs")

        write.payload.foldLeft(store.size()) { case (offset, repr) =>
          val bytes = FileJournalSerialization.toProto(repr, serialization)

          index.appendEntry(offset, bytes.length)
          store.appendBytes(bytes)

          offset + bytes.length
        }

        ()
      }
    }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

      if (toSequenceNr > Int.MaxValue)
        Future.failed(new IllegalArgumentException(sequenceNrMaxMessage))

      Future {

        val seq = toSequenceNr.toInt

        val offset = indexFile(persistenceId).readIndex(seq, seq).apply(0)._1

        storeFile(persistenceId).truncate(offset)
        indexFile(persistenceId).truncate(indexSize * toSequenceNr)

        ()
      }
    }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = {

    if (fromSequenceNr > Int.MaxValue)
      Future.failed(new IllegalArgumentException(sequenceNrMaxMessage))

    if (toSequenceNr > Int.MaxValue)
      Future.failed(new IllegalArgumentException(sequenceNrMaxMessage))

    val count = Math.min(Int.MaxValue, max).toInt

    if (count == 0)
      Future.successful(())
    else
      Future {

        val index = indexFile(persistenceId)
        val store = storeFile(persistenceId)

        println(s"read : $persistenceId : from: ${fromSequenceNr}, to: ${toSequenceNr}, count: $count")

        val offsets = index.readIndex(fromSequenceNr.toInt, toSequenceNr.toInt, count)

        offsets.zipWithIndex.foreach { case ((offset, size), i) =>

          val bytes = store.readBytes(offset, size)
          val seq = fromSequenceNr + i
          val persistentRepr = FileJournalSerialization.toDomain(bytes, serialization, persistenceId, seq).get

          recoveryCallback(persistentRepr)
        }
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {

    Future { indexFile(persistenceId).size / indexSize }
  }
}
