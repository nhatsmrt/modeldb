package ai.verta.repository

import ai.verta.client._
import ai.verta.blobs._
import ai.verta.blobs.dataset._

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls
import scala.util.{Try, Success, Failure}

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path}
import java.util.{Arrays, Random}

import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.prop._

class TestCommitProperty extends FunSuite with Checkers {
  implicit val ec = ExecutionContext.global

  def fixture =
    new {
        val client = new Client(ClientConnection.fromEnvironment())
        val repo = client.getOrCreateRepository("My Repo").get

        val pathBlob = PathBlob("./src/test/scala/ai/verta/blobs/testdir", true).get
        val s3Blob = S3(S3Location("s3://verta-scala-test/testdir/").get, true).get

        val commit = repo.getCommitByBranch()
          .flatMap(_.update("abc", s3Blob))
          .flatMap(_.update("def", pathBlob))
          .flatMap(_.save("some-msg")).get
    }

  def cleanup(
    f: AnyRef{val client: Client; val repo: Repository; val commit: Commit; val pathBlob: PathBlob; val s3Blob: S3}
  ) = {
    deleteDirectory(new File("./somefiles"))
    deleteDirectory(new File("./somefiles2"))

    (new File("./somefile")).delete()
    (new File("./somefile2")).delete()

    f.client.deleteRepository(f.repo.id)
    f.client.close()
  }

  /** Delete the directory */
  def deleteDirectory(dir: File): Unit = {
    Option(dir.listFiles()).map(_.foreach(deleteDirectory))
    dir.delete()
  }

  /** Check to see if two files have the same content */
  def checkEqualFile(firstFile: File, secondFile: File) = {
    val first: Array[Byte] = Files.readAllBytes(firstFile.toPath)
    val second = Files.readAllBytes(secondFile.toPath)
    Arrays.equals(first, second)
  }

  /** Generate a random file to the given path */
  def generateRandomFile(path: String, seed: Long, size: Int = 1024 * 1024): Try[Array[Byte]] = {
    val random = new Random(seed)
    val contents = new Array[Byte](size)
    random.nextBytes(contents)

    val file = new File(path)
    var fileStream: Option[FileOutputStream] = None

    try {
      Try({
        Option(file.getParentFile()).map(_.mkdirs()) // create the ancestor directories, if necessary
        file.createNewFile()

        fileStream = Some(new FileOutputStream(file, false)) // overwrite, if already exists
        fileStream.get.write(contents)
      }).map(_ => contents)
    } finally {
      if (fileStream.isDefined)
        fileStream.get.close()
    }
  }

  test("Property: downloading a versioned blob should recover the original content") {
    check((firstSeed: Long, secondSeed: Long) => {
      val f = fixture

      try {
        val originalContent = generateRandomFile("somefile", firstSeed).get
        val pathBlob = PathBlob("somefile", true).get
        val commit = f.commit
          .update("file", pathBlob)
          .flatMap(_.save("some-msg")).get

        // create a new file with same name
        val updatedContent = generateRandomFile("somefile", secondSeed).get
        // check that the content is now different:
        val contentIsUpdated =
          !Files.readAllBytes((new File("somefile")).toPath).sameElements(originalContent)

        // recover the old versioned file:
        val retrievedBlob: Dataset = commit.get("file").get match {
          case path: PathBlob => path
        }
        retrievedBlob.download(Some("somefile"), "somefile")
        val contentIsRestored =
          Files.readAllBytes((new File("somefile")).toPath).sameElements(originalContent)

        if(!((originalContent.sameElements(updatedContent)) || (contentIsUpdated && contentIsRestored))) {
          println(firstSeed)
          println(secondSeed)
          println(contentIsUpdated && contentIsRestored)
        }

        (originalContent.sameElements(updatedContent)) || (contentIsUpdated && contentIsRestored)
      } finally {
        cleanup(f)
      }
    }, minSuccessful(100))
  }
}
