package example

import ai.verta.client._
import ai.verta.client.entities._
import ai.verta.repository._
import ai.verta.blobs._
import ai.verta.blobs.dataset._

import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}

object ThursdayDemo extends App {
  implicit val ec = ExecutionContext.global

  val client = new Client(ClientConnection.fromEnvironment())
  var repoGetAttempt: Option[Repository] = None
  var projectGetAttempt: Option[Project] = None

  try {
    // create a repository
    val repo = client.getOrCreateRepository("DemoRepository").get
    repoGetAttempt = Some(repo)

    // Create some blobs:
    val pathBlob = PathBlob(f"${System.getProperty("user.dir")}/src/test/scala/ai/verta/blobs/testdir").get

    // Save the blobs in a commit and save the commit to ModelDB:
    val originalCommit = repo.getCommitByBranch()

    val branchA = originalCommit.flatMap(_.newBranch("branch-a"))
      .flatMap(_.update("some-path-1", pathBlob))
      .flatMap(_.update("a/b/some-path-2", pathBlob))
      .flatMap(_.save("add the blobs")).get

    val branchB = originalCommit.flatMap(_.newBranch("branch-b"))
      .flatMap(_.update("a/b/some-path-3", pathBlob))
      .flatMap(_.update("a/c/some-path-4", pathBlob))
      .flatMap(_.save("add the blobs")).get

    val mergedCommit = branchA.merge(branchB).get


    val project = client.getOrCreateProject("Thursday project").get
    projectGetAttempt = Some(project)
    val expRun = project.getOrCreateExperiment("Thursday experiment")
                        .flatMap(_.getOrCreateExperimentRun()).get
    expRun.logCommit(mergedCommit)

    val retrievedCommit = expRun.getCommit().get.commit
    assert(retrievedCommit equals mergedCommit)

    // Type error:
    expRun.logMetric("int-metric", 4)
    expRun.logMetric("double-metric", 3.5)
    expRun.logMetric("string-metric", "some-string")

    assert(expRun.getMetric("int-metric").get.get.asBigInt.get equals 4)
    assert(expRun.getMetric("double-metric").get.get.asDouble.get equals 3.5)
    assert(expRun.getMetric("string-metric").get.get.asString.get equals "some-string")

    val someTry = (for (
      _ <- Success(())
    ) yield {
      Integer.parseInt("abc")
    })
    println(someTry)

    // expRun.logMetric("dummy-metric", DummyArtifact("please fail"))
    // expRun.logMetric("boolean-metric", false)

  } finally {
    if (repoGetAttempt.isDefined)
      client.deleteRepository(repoGetAttempt.get.id)

    if (projectGetAttempt.isDefined)
      client.deleteProject("Thursday project")
    client.close()
  }
}
