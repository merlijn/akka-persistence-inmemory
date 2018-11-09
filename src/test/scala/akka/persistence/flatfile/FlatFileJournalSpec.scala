package akka.persistence.flatfile

import java.io.File
import java.nio.file.{Files, Path, Paths}

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import FlatFileJournalSpec._
import FileUtil._

object FlatFileJournalSpec {

  val config = ConfigFactory.parseString(
    """
      |akka.persistence {
      |    journal.plugin = "flatfile-journal"
      |    snapshot-store.plugin = "inmemory-snapshot-store"
      |  }
      | flatfile-journal {
      |   path = "/tmp/akka-journal"
      | }
      |
    """.stripMargin)
}

class FlatFileJournalSpec extends JournalSpec(config) {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false

  def recursiveDelete(file: File): Unit = {
    import java.io.IOException

    if (file.exists() && file.isDirectory) {
      for (childFile <- file.listFiles) {
        if (childFile.isDirectory) recursiveDelete(childFile)
        else if (!childFile.delete) throw new IOException(s"Failed to delete file: ${childFile.getName}")
      }
      if (!file.delete) throw new IOException(s"Failed to delete file: ${file.getName}")
    }
  }

  override protected def beforeAll(): Unit = {

    Paths.get(config.getString(s"flatfile-journal.path")).deleteRecursive()

    super.beforeAll()
  }
}