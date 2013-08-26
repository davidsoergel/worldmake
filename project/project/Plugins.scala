import sbt._
import sbt.Keys._


object IeslPluginLoader extends Build {

  // automatically updating SNAPSHOTs does not seem to work right here.
  // as a workaround, we'll just release every incremental change to iesl-sbt-base with integer build numbers.

  lazy val root = Project(id = "plugins", base = file("."))
    .settings(resolvers += "IESL Public Releases" at "https://dev-iesl.cs.umass.edu/nexus/content/groups/public")
    //.settings(resolvers += "IESL Public Snapshots" at "https://dev-iesl.cs.umass.edu/nexus/content/groups/public-snapshots")
    .settings(addSbtPlugin("edu.umass.cs.iesl" %% "iesl-sbt-base" % "latest.release")) // apparently buggy: "latest.integration" changing()
}


// as of sbt 0.12.0 we can rebuild the plugin on the fly from the hg repository,
// avoiding the Nexus URL chicken-and-egg problem (or rather, pushing it back one level to the Bitbucket URL)

// ah, but sbt apparently doesn't hg update things in ~/.sbt/staging

/*
object IeslPluginLoader extends Build {
  override lazy val projects = Seq(root)
  lazy val root = Project("plugins", file(".")) dependsOn( ieslSbtBase )
  lazy val ieslSbtBase = uri("hg:https://IESL@bitbucket.org/IESL/iesl-sbt-base")
}

*/
