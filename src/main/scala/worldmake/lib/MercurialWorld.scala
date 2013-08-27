package worldmake.lib

import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.{WorldMakeConfig, Derivation}
import WorldMakeConfig.WMHash
import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
abstract class MercurialWorld {

  protected val reposRequestedVersions: Map[String, (String, String)]

  private lazy val reposActualVersions: Map[String, String] = reposRequestedVersions.map({
    case (k, (b, "latest")) => (k, MercurialWorkspaces.getLatestVersions(k)(b))
    case (k, (b, v)) => (k, v)
  })

  lazy val workingDirs: Map[String, Derivation[Path]] = reposActualVersions.map({
    case (k: String, v: String) => (k, MercurialWorkspaces.get(k, v))
  })

  lazy val inputHash = Hash.toHex(WMHash(reposActualVersions.map({
    case (k: String, v: String) => k + v
  }).mkString))

  val targets: Map[String, Derivation[_]]
}
