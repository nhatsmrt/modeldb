package ai.verta._repository

import ai.verta.client.{getPersonalWorkspace, urlEncode}
import ai.verta.blobs._
import ai.verta.swagger.client.ClientSet
import ai.verta.swagger._public.modeldb.versioning.model._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.HashMap
import scala.annotation.switch

/** Commit within a ModelDB Repository
 *  There should not be a need to instantiate this class directly; please use Repository.getCommit methods
 *  TODO: privatize blobs
 *  TODO: clean up blobs retrieval
 *  TODO: write tests for interactions with blobs: load, update, get, save, remove etc.
 */
class Commit(
  val clientSet: ClientSet, val repo: VersioningRepository,
  var commit: VersioningCommit, var commit_branch: Option[String] = None
) {
  var saved = true // whether the commit instance is saved to database
  var loaded_from_remote = false // whether blob has been retrieved from remote
  var blobs = new HashMap[String, VersioningBlob]() // mutable map for storing blobs

  /** Retrieve commit's blobs from remote
   */
  def load_blobs()(implicit ec: ExecutionContext): Unit = {
    if (!loaded_from_remote) {
      val ids: List[String] = commit.commit_sha match {
        case Some(v) => List(v)
        case None => commit.parent_shas.get
      }

      ids.map(id => load_blobs_from_id(id))
      loaded_from_remote = true
    }
  }

  /** Retrieve blobs associated to commit with given id and update blobs
   *  @param id id of the commit
   */
  private def load_blobs_from_id(id: String)(implicit ec: ExecutionContext): Try[List[Option[VersioningBlob]]] = {
    clientSet.versioningService.ListCommitBlobs2(
      commit_sha = id,
      repository_id_repo_id = repo.id.get
    ) // Try[VersioningListCommitBlobsRequestResponse]
    .map(_.blobs) // Try[Option[List[VersioningBlobExpanded]]]
    .map(ls => if (ls.isEmpty) null else ls.get.map(
      blob => {
        var joinedLocation = blob.location.get.mkString("/")
        blobs.put(joinedLocation, blob.blob.get)
      }
    ))
  }

  /** Retrieves the blob at path from this commit
   *  @param path location of a blob
   *  @return ModelDB versioning blob
   */
  def get(path: String)(implicit ec: ExecutionContext): Option[Blob] = {
    load_blobs()
    blobs.get(path).map(versioningBlobToBlob)
  }

  /** Adds blob to this commit at path
   *  If path is already in this Commit, it will be updated to the new blob
   *  @param path Location to add blob to
   *  @param blob ModelDB versioning blob.
   */
  def update[T <: Blob](path: String, blob: T)(implicit ec: ExecutionContext): Unit = {
    load_blobs()
    blobs.put(path, blob.versioningBlob)
    becomeChild()
  }

  /** Deletes the blob at path from this commit and return it
   *  @param path Location of the blob to removed
   *  @return the blob (if existed at path), or None
   */
  def remove(path: String)(implicit ec: ExecutionContext): Option[Blob] = {
    load_blobs()
    becomeChild()
    blobs.remove(path).map(versioningBlobToBlob)
  }

  /** Saves this commit to ModelDB
   *  @param message description of this commit
   *  TODO: implement this method
   *  TODO: incorporate diff
   *  TODO: write tests for this method
   */
  def save(message: String)(implicit ec: ExecutionContext) = {
    if (!saved) {
      clientSet.versioningService.CreateCommit2(
        body = VersioningCreateCommitRequest(
          commit = Some(withMessage(message)),
          blobs = Some(blobsList)
        ),
        repository_id_repo_id = repo.id.get
      )
      .map(r => if (!r.commit.isEmpty) {
        commit = r.commit.get
        commit_branch.map(setBranch(_))
        init()
      })
    }
  }

  /** Return ancestors, starting from this Commit until the root of the Repository
   *  @return a list of ancestors
   */
  def log()(implicit ec: ExecutionContext): Try[List[Commit]] = {
    clientSet.versioningService.ListCommitsLog4(
      repository_id_repo_id = repo.id.get,
      commit_sha = commit.commit_sha.get
    ) // Try[VersioningListCommitsLogRequestResponse]
    .map(_.commits) // Try[Option[List[VersioningCommit]]]
    .map(ls => if (ls.isEmpty) null else ls.get.map(c => new Commit(clientSet, repo, c))) // Try[List[Commit]]
  }

  /** Assigns a tag to this Commit
   *  @param tag tag
   */
  def tag(tag: String)(implicit ec: ExecutionContext) = {
    if (!saved) {
      throw new IllegalStateException("Commit must be saved before it can be tagged")
    }
    else {
      clientSet.versioningService.SetTag2(
        body = "\"" + commit.commit_sha.get + "\"",
        repository_id_repo_id = repo.id.get,
        tag = urlEncode(tag)
      )
    }
  }

  /*** HELPER METHODS ***/

  /** Become child of current commit (if current commit is saved)
   */
  private def becomeChild() = {
    if (saved) {
      commit = VersioningCommit(
        parent_shas = commit.commit_sha.map(List(_))
      )
      saved = false
    }
  }

  /** Convert a location to "repeated string" representation
      Based on Python's implementation
      @param path path
      @return the repeated string representation of the path
   */
   private def pathToLocation(path: String): List[String] = {
     if (path.charAt(0) == '/') pathToLocation(path.substring(1))
     else path.split("/").toList
   }

   /** Convert the dictionary of blobs into list form for API requests
    *  @return the list required
    */
  private def blobsList: List[VersioningBlobExpanded] = {
    (for ((path, blob) <- blobs) yield VersioningBlobExpanded(
        blob = Some(blob),
        location = Some(pathToLocation(path))
    )).toList
  }

  /** Add message to current commit. Done before saving
   *  @param message message
   */
  private def withMessage(message: String) = VersioningCommit(
        author = commit.author,
        commit_sha = commit.commit_sha,
        message = Some(message),
        parent_shas = commit.parent_shas
  )

  /** Reset the state of commit
   */
  private def init() = {
    saved = true
    // loaded_from_remote = false
    // blobs = new HashMap[String, VersioningBlob]()
  }

  /** Set the commit of named branch to current commit
   *  @param branch branch
   */
  private def setBranch(branch: String)(implicit ec: ExecutionContext) = {
    clientSet.versioningService.SetBranch2(
      body = "\"" + commit.commit_sha.get + "\"",
      branch = branch,
      repository_id_repo_id = repo.id.get
    ) match {
      case Success(_) => {commit_branch = Some(branch)}
      case Failure(_) => {}
    }
  }
}
