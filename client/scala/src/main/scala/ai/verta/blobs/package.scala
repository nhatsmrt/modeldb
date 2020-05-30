package ai.verta

import ai.verta.swagger._public.modeldb.versioning.model._

package object blobs {
  /** Helper function to convert a VersioningBlob instance to corresponding Blob subclass instance
   *  @param vb the VersioningBlob instance
   *  @return an instance of a Blob subclass
   *  TODO: finish the pattern matching with other blob subclasses
   */
  def versioningBlobToBlob(vb: VersioningBlob): Blob = vb match {
    case VersioningBlob(Some(VersioningCodeBlob(Some(git), _)), _, _, _) => new Git(git)

    case VersioningBlob(_, _, Some(VersioningDatasetBlob(Some(path), _)), _) => new PathBlob(path)

    case VersioningBlob(_, _, Some(VersioningDatasetBlob(_, Some(s3))), _) => new S3(s3)

    case _ => Git()
  }


  /** Analogous to os.path.expanduser
   *  From https://stackoverflow.com/questions/6803913/java-analogous-to-python-os-path-expanduser-os-path-expandvars
   *  @param path path
   *  @return path, but with (first occurence of) ~ replace with user's home directory
   */
  def expanduser(path: String) = path.replaceFirst("~", System.getProperty("user.home"))

  // def autocaptureGit() = {
  //
  // }
}
