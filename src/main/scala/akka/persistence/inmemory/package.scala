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

package akka.persistence

import java.util.UUID

import akka.persistence.inmemory.util.UUIDs
import akka.persistence.journal.Tagged
import akka.persistence.query.TimeBasedUUID

import scala.collection.immutable._
import scala.compat.Platform

package object inmemory {
  type Seq[A] = scala.collection.immutable.Seq[A]

  def now: Long = Platform.currentTime
  def nowUuid: UUID = UUIDs.timeBased()
  def getTimeBasedUUID: TimeBasedUUID = TimeBasedUUID(nowUuid)

  implicit class PersistentReprAdditions(repr: PersistentRepr) {
    def getTags: Set[String] = {
      repr.payload match {
        case Tagged(_, tags) => tags
        case _               => Set.empty[String]
      }
    }
  }

  final case class JournalEntry(
      persistenceId: String,
      sequenceNr: Long,
      serialized: Array[Byte],
      tags: Set[String],
      deleted: Boolean = false,
      timestamp: TimeBasedUUID = getTimeBasedUUID,
      offset: Option[Long] = None)

  final case class SnapshotEntry(
      persistenceId: String,
      sequenceNumber: Long,
      created: Long,
      snapshot: Array[Byte])

  implicit def seqToVector[A](xs: Seq[A]): Vector[A] = xs.toVector
  implicit def setToVector[A](xs: Set[A]): Vector[A] = xs.toVector
  implicit def mapSeqToVector[K, V](map: Map[K, Seq[V]]): Map[K, Vector[V]] = map.mapValues(_.toVector)
}
