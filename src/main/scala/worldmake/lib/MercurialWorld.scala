package worldmake.lib

import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.lib.MercurialWorkspaces
import worldmake.{Derivation, ExternalPathDerivation}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
abstract class MercurialWorld {
  
  protected val reposRequestedVersions: Map[String, (String, String)]
  
  private val reposActualVersions: Map[String, String] = reposRequestedVersions.map({
    case (k, (b, "latest")) => (k, MercurialWorkspaces.getLatestVersions(k)(b))
    case (k, (b, v)) => (k, v)
  })
  
  val workingDirs: Map[String, ExternalPathDerivation] = reposActualVersions.map({
    case (k: String, v: String) => (k, MercurialWorkspaces.get(k, v))
  })

  val inputHash = Hash.toHex(WMHash(reposActualVersions.map({
    case (k: String, v: String) => k + v
  }).mkString))
  
  val targets:Map[String,Derivation[_]]
}
