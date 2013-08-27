import sbt._
import edu.umass.cs.iesl.sbtbase.Dependencies
import edu.umass.cs.iesl.sbtbase.IeslProject._

object WorldMakeBuild extends Build {

  val vers = "0.1-SNAPSHOT"

  implicit val allDeps: Dependencies = new Dependencies(); //(CleanLogging.excludeLoggers)  // doesn't work?

  import allDeps._

  //override def settings = super.settings ++ org.sbtidea.SbtIdeaPlugin.ideaSettings

  val deps = Seq(ieslScalaCommons("latest.integration") ,
    scalaIoFile("0.4.2"),
    typesafeConfig(), 
    "org.mongodb" %% "casbah" % "2.5.0",
    scalazCore("7.0.0"),
    "org.scalaz" %% "scalaz-concurrent" % "7.0.0",
    scalatime()
  )
/*    dsutils(),
    commonsIo(),
    classutil(),
    scalaCompiler(),
    scalatest(),
    specs2(),
    scalaIoCore("0.4.0"),
    scalaIoFile("0.4.0"),
    "com.typesafe" % "config" % "latest.release", // TODO allDeps.typesafeConfig(),
    jdom("1.1.3"),
    mavenCobertura(),
    mavenFindbugs())*/

  lazy val worldmake = Project("worldmake", file(".")).ieslSetup(vers, deps, Public, WithSnapshotDependencies).cleanLogging.standardLogging

}
