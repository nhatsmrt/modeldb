package ai.verta.client

import ai.verta.repository._
import ai.verta.blobs._
import scala.language.reflectiveCalls
import scala.concurrent.ExecutionContext

import org.scalatest.FunSuite
import org.scalatest.Assertions._

class TestExperimentRun extends FunSuite {
  implicit val ec = ExecutionContext.global

  def fixture =
    new {
        val client = new Client(ClientConnection.fromEnvironment())
    }

  def cleanup(f: AnyRef{val client: Client}) = {
    f.client.close()
  }

  test("log and get commit") {
    val f = fixture
    val repo = f.client.getOrCreateRepository("ExpRun Repo").get
    val commit = repo.getCommitByBranch()

    try {
      val workingDir = System.getProperty("user.dir")
      val testDir = workingDir + "/src/test/scala/ai/verta/blobs/testdir/testfile"
      var path = PathBlob(List(testDir))

      commit.get.update("abc/def", path)
      assert(commit.get.save("Add a blob").isSuccess)

      val expRun = f.client.getOrCreateProject("scala test")
        .flatMap(_.getOrCreateExperiment("experiment"))
        .flatMap(_.getOrCreateExperimentRun()).get

      val logAttempt =
        expRun.logCommit(commit.get, Some(Map[String, String]("mnp/qrs" -> "abc/def")))

      assert(logAttempt.isSuccess)

      val getCommitAttempt =
        expRun.getCommit()

      assert(getCommitAttempt.isSuccess)

      val commitKeyPaths = getCommitAttempt.get
      assert(commitKeyPaths.commitSHA.equals(commit.get.commit.commit_sha.get))
      assert(commitKeyPaths.keyPaths.get.contains(("mnp/qrs")))
    } finally {
      f.client.deleteRepository(repo.repo.id.get.toString)
      cleanup(f)
    }
  }
}
