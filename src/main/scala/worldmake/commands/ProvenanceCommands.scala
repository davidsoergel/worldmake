package worldmake.commands

import worldmake._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.storage.{StoredProvenancesForRecipe, Identifier, Storage}
import scalax.file.Path

import scalax.io.{Output, Resource}
import scala.collection.GenSet


class MultiProvenanceCommands[T](out: Output, provenances: GenSet[Provenance[T]]) {
  def showProvenances() {
    provenances.map(p => out.write(p.statusLine + "\n"))
  }

  def showProvenanceDetailed() {
    provenances.map(p => out.write(p.statusBlock + "\n\n"))
  }
}

class SingleProvenanceCommands[T](out: Output, p: Provenance[T]) {

  def showProvenanceQueue() {
    p.queue.map((x: Provenance[_]) => out.write(x.statusLine + "\n"))
  }

  def showProvenanceDeps() {
    p match {
      case d: DependenciesBoundProvenance[T] => {
        d.derivedFromAll.map(x => out.write(x.statusLine + "\n"))
      }
      case _ => out.write(s"Provenance ${p.provenanceId} has no dependencies.")
    }
  }

  def showDeps() {
    p match {
      case b: DependenciesBoundProvenance[_] => {
        b.derivedFromNamed.map({
          case (n, s) => out.write(s"$n%16s => $s.statusLine%s")
        })

        if (b.derivedFromNamed.nonEmpty && b.derivedFromUnnamed.nonEmpty) {
          out.write("\nUnnamed Dependencies:\n")
        }
        val space=" "
        b.derivedFromUnnamed.map(s => out.write(f" $space%16s => ${s.statusLine}%s"))
      }
      case _ => out.write("Provenance does not yet have bound dependencies.\n")
    }
    p.queue.map((x: Provenance[_]) => out.write(x.statusLine + "\n"))
  }

  def showProvenanceQueueDetailed() {
    p.queue.map((x: Provenance[_]) => out.write(x.statusBlock + "\n"))
  }

  def verifyProvenanceInputs() {
    p.queue.collect({case x : ConstantProvenance[Any] => x }).map(x => {
      Storage.provenanceStore.verifyContentHash(x.provenanceId)
      out.write(s"Input hash verified for provenance ${x.provenanceId}\n")
    })
  }

  def showProvenanceBlame() {
    val ff = p.queue.collect({
      case x: FailedProvenance[_] => x
    })
    for (f <- ff) {
      out.write(f.statusLine + "\n")
    }
  }

  /*
      def showProvenanceTree()  {
        out.write(strategy.tracker.printTree(recipe, ""))
      }
  */

  def showProvenanceLog() {
    //}, strategy: LifecycleAwareCookingStrategy) = {
    // for (p <- provenances) {
    out.write(p.statusBlock + "\n")
    p match {
      case prp: PostRunProvenance[_] => {
        out.write("\n LOG \n================\n")
        prp.printLogHeadTail(Resource.fromOutputStream(System.out))
      }
    }
    //}
  }

  def showProvenanceFullLog() {
    //}, strategy: LifecycleAwareCookingStrategy) = {
    // for (p <- provenances) {
    out.write(p.statusBlock + "\n")
    p match {
      case prp: PostRunProvenance[_] => {
        out.write("\n LOG \n================\n")
        prp.printLog(Resource.fromOutputStream(System.out))
      }
    }
    //}
  }

}


/*
object ProvenanceFinder {
  def apply(world: World, s: String): Set[WorldmakeEntity] = {
    EntityFinder(world, s).map({
      case r: Recipe[_] =>
      case p: Provenance[_] => p
      case a: Artifact[_] => Storage.provenanceStore.getWithArtifactById(a.constantId)
    })
  }
}
*/

object ProvenanceFinder extends Logging {
  def apply[T](recipe: Recipe[T]): GenSet[Provenance[T]] = {

    //val namedRecipe = world.get(s)

    // we do not allow finding unnamed recipes, even if their IDs are already in the database,
    // because that would require exhaustively creating all the recipes in the world in order to compute their hash IDs.

    val spfr : StoredProvenancesForRecipe[T] = StoredProvenancesForRecipe(recipe.recipeId)
    /*
      if(spfr.all.isEmpty) {
        //strategy.stageOne(r)
        logger.warn(s"Found recipe named '$s', but it has no provenances. Need to enqueue it first?")

        //StoredProvenancesForRecipe(r.recipeId)
      }
      */
    spfr.all
  }

  def apply(world: World, s: String): Set[Provenance[_]] = {
    // test all possibilities to look for ambiguity

    val uuidProvenance : Option[Provenance[Any]] = Storage.provenanceStore.get(new Identifier[Provenance[Any]](s)) // UUID string
    val artifactA : Set[Provenance[_]] = Storage.provenanceStore.findWithArtifactFileByIdFragment(s)
    val artifactB : Set[Provenance[_]] = Storage.provenanceStore.findWithArtifactByValue(s)
    val pathArtifact : Set[Provenance[_]] = Storage.provenanceStore.findWithArtifactByValue(Path.fromString(s).toAbsolute.path)

    val foundEntities: Set[Provenance[_]] = Seq(uuidProvenance.toSet, artifactA, artifactB, pathArtifact).flatten.toSet
    foundEntities
  }
}
