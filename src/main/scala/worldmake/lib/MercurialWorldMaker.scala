package worldmake.lib

import worldmake._
import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object MercurialWorldMaker {


  /*
    private
    lazy val inputHash = Hash.toHex(WMHash(reposActualVersions.map({
      case (k: String, v: String) => k + v
    }).mkString))
  */
  //val targets: Map[String, Derivation[_]]


  def apply(reposRequestedVersions: Map[String, (String, String)]): World = {
    lazy val reposActualVersions: Map[String, String] = reposRequestedVersions.map({
      case (k, (b, "latest")) => (k, MercurialWorkspaces.getLatestVersions(k)(b))
      case (k, (b, v)) => (k, v)
    })

    lazy val workingDirs: Map[String, Derivation[Path]] = reposActualVersions.map({
      case (k: String, v: String) => (k, MercurialWorkspaces.get(k, v))
    })
    new PathWorld(workingDirs)
  }
}


