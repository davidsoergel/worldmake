package worldmake


import com.typesafe.scalalogging.slf4j.Logging
import scala.sys.process.{ProcessLogger, Process}
import scalax.io.Resource
import java.util.UUID
import org.joda.time.DateTime
import worldmake.storage.Identifier

//import java.lang.ProcessBuilder.Redirect

import WorldMakeConfig._

//import scala.collection.JavaConversions._

import scalax.file.Path
import java.io.File

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

object SystemDerivation {
  // "toEnvironmentString" is not a method of the Derivation trait because the Any->String conversion may differ by 
  // context (at least, eg., # of sig figs, or filename vs file contents, etc.)
  // For that matter, what if it differs for different arguments of the same type? 
  def toEnvironmentString[T](x: Artifact[T]): String = x match {
    case f: ExternalPathArtifact => f.abspath
    case f: TraversableArtifact[T] => f.artifacts.map((x: Artifact[_]) => toEnvironmentString(x)).mkString(" ")
    //case f:GenTraversableArtifact => f.artifacts.map(toEnvironmentString).mkString(" ")
    case f => f.value.toString
  }

}

class SystemDerivation(val script: Derivation[String], namedDependencies: Map[String, Derivation[_]]) extends ExternalPathDerivation with DerivableDerivation[Path] with Logging {

  // todo: include self version number??
  lazy val derivationId = {
    val dependencyInfos: Seq[String] = namedDependencies.map({
      case (k, v) => k.toString + v.derivationId.s
    }).toSeq.sorted
    Identifier[Derivation[Path]](WMHashHex(script.derivationId.s + dependencyInfos.mkString("")))
  }

  val description = "result of: " + script.description

  val dependencies = namedDependencies.values.toSet


  // todo store provenance lifecycle

  def derive = synchronized {
    import SystemDerivation._

    val startTime = DateTime.now()
    val outputPath: Path = fileStore.newPath
    val workingDir = Path.createTempDirectory()
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)

    val logWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)

    val reifiedDependencies = namedDependencies.mapValues(_.resolveOne)
    val dependenciesEnvironment: Map[String, String] = reifiedDependencies.mapValues(x => toEnvironmentString(x.artifact))
    val environment: Map[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)

    val runner = Resource.fromFile(new File((workingDir / "worldmake.runner").toAbsolute.path))
    runner.write(script.resolveOne.output.get.value)

    val envlog = Resource.fromFile(new File((workingDir / "worldmake.environment").toAbsolute.path))
    envlog.write(environment.map({
      case (k, v) => k + " = " + v
    }).mkString("\n"))

    val pb = Process(Seq("/bin/sh", "./worldmake.runner"), workingDir.jfile, environment.toArray: _*)

    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val pbLogger = ProcessLogger(
      (o: String) => logWriter.write(o),
      (e: String) => logWriter.write(e))

    val exitCode = pb ! pbLogger

    // todo: detect retained dependencies like Nix

    val result = ExternalPathArtifact(outputPath)

    if (exitCode != 0) {
      logger.warn("Deleting output directory: " + outputPath)
      outputPath.deleteRecursively()
      logger.warn("Retaining working directory: " + workingDir)
      throw new FailedDerivationException

      // todo store failure log
    }

    if (WorldMakeConfig.debugWorkingDirectories) {
      logger.warn("Retaining working directory: " + workingDir)
    } else {
      workingDir.deleteRecursively()
    }
    val endTime = DateTime.now()

    SuccessfulProvenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
      derivationId = SystemDerivation.this.derivationId,
      status = ProvenanceStatus.Success,
      derivedFromNamed = reifiedDependencies,
      startTime = startTime,
      endTime = endTime,
      statusCode = Some(exitCode),
      log = Some(logWriter),
      output = Some(result))
  }
}
