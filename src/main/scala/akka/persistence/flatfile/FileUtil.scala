package akka.persistence.flatfile

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}

object FileUtil {

  implicit class RandomAccessFns(path: Path) {

    import java.nio.ByteBuffer
    import java.nio.channels.FileChannel

    def withAutoCloseChannel[T](fc: FileChannel)(fn: => T):T = {
      try {
        fn
      } finally {
        if (fc != null) fc.close()
      }
    }

    def createFileIfNotExists(): Path = {
      if (!Files.exists(path))
        Files.createFile(path)

      path
    }

    def createDirectoryIfNotExists(): Path = {
      if (!Files.exists(path))
        Files.createDirectory(path)

      path
    }

    def deleteRecursive() = {
      import java.io.IOException

      def deleteFile(file: File): Unit = {

        if (file.exists() && file.isDirectory) {
          for (childFile <- file.listFiles) {
            if (childFile.isDirectory) deleteFile(childFile)
            else if (!childFile.delete) throw new IOException(s"Failed to delete file: ${childFile.getName}")
          }
          if (!file.delete) throw new IOException(s"Failed to delete file: ${file.getName}")
        }
      }

      deleteFile(path.toFile)
    }

    def readIndex(from: Int, to: Int, max: Int = Int.MaxValue): Seq[(Long, Int)] = {

      val offset = (from - 1) * 12
      val limit = Math.min(to - from + 1, max)
      val bytesToRead = limit * 12

      try {

        val buffer = readAt(offset, bytesToRead)

//        println(s"from: $from, to: $to, max: $max, offset: $offset, bytesToRead: $bytesToRead, size: ${size()}, remaining: ${buffer.remaining()}")
        (1 to limit).map(_ => buffer.getLong -> buffer.getInt)
      } catch {
        case e: Exception =>

          e.printStackTrace()
          throw e;
      }
    }

    def appendEntry(offset: Long, size: Int): Unit = {

      val buffer = ByteBuffer.allocate(12)
      buffer.putLong(offset)
      buffer.putInt(size)
      buffer.rewind()
      append(buffer)
    }

    def readLastEntry(): (Long, Int) = {
      val buffer = readTail(12)
      buffer.getLong() -> buffer.getInt()
    }

    def readInt(position: Long): Int = {
      readAt(position, 4).getInt
    }

    def appendInt(int: Int) = {
      val buffer = ByteBuffer.allocate(4)
      buffer.putInt(int)
      buffer.rewind()
      append(buffer)
    }

    def append(buffer: ByteBuffer) = {
      val fc = FileChannel.open(path, StandardOpenOption.APPEND)

      withAutoCloseChannel(fc) {
        while (buffer.hasRemaining)
          fc.write(buffer)

        buffer.rewind()
      }
    }

    def appendBytes(bytes: Array[Byte]): Unit = {
      append(ByteBuffer.wrap(bytes))
    }

    def readTail(nrOfBytes: Int): ByteBuffer =
      readAt(size() - nrOfBytes, nrOfBytes)

    def readAt(position: Long, nrOfBytes: Int): ByteBuffer = {
      val copy = ByteBuffer.allocate(nrOfBytes)

      val fc = FileChannel.open(path, StandardOpenOption.READ)

      withAutoCloseChannel(fc) {
        fc.position(position)
        var nread = 0
        do { nread = fc.read(copy) } while (nread != -1 && copy.hasRemaining)
      }

      copy.rewind()
      copy
    }

    def truncate(position: Long) = {
      val fc = FileChannel.open(path, StandardOpenOption.WRITE)

      withAutoCloseChannel(fc) {
        fc.truncate(position)
      }
    }

    def readBytes(position: Long, nrOfBytes: Int): Array[Byte] = {
      readAt(position, nrOfBytes).array()
    }

    /**
      * The size of the file in bytes.
      */
    def size(): Long = {
      path.toFile.length()
    }
  }
}
