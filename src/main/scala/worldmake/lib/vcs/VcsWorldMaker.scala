package worldmake.lib.vcs

import worldmake._
import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class VcsWorldMaker(workspaces:VcsWorkspaces) {


  /*
    private
    lazy val inputHash = Hash.toHex(WMHash(reposActualVersions.map({
      case (k: String, v: String) => k + v
    }).mkString))
  */
  //val targets: Map[String, Derivation[_]]


  def apply(reposRequestedVersions: Map[String, (String, String)]): World = {
    lazy val reposActualVersions: Map[String, String] = reposRequestedVersions.map({
      case (k, (b, "latest")) => (k, workspaces.getLatestVersions(k)(b))
      case (k, (b, v)) => (k, v)
    })

    lazy val workingDirs: Map[String, Recipe[ManagedPath]] = reposActualVersions.map({
      case (k: String, v: String) => (k, workspaces.get(k, v))
    })
    new PathWorld(workingDirs)
  }
}


