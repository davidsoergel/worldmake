/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.storage

import worldmake._
import worldmake.WorldMakeConfig._
import com.typesafe.scalalogging.slf4j.Logging


sealed trait PathArtifact[T <: PathReference] extends Artifact[T] {
  def path = value.path

  lazy val abspath = path.toAbsolute.path
  lazy val basename = path.name

  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override lazy val environmentString = abspath

  // todo just pass in the child hashes for an assembly

  override lazy val contentHashBytes: Option[Array[Byte]] = if (aggressiveHashing) {
    val result = {
      if (path.isFile) WMHash(path.fileOption.get)
      else {
        // this doesn't take into account the filenames directly, but does concatenate the child hashes in filename-sorted order. 
        // todo store the child hashes in the DB for reuse?
        WMHash(children.par.map(p => p.basename + p.contentHash).mkString)
      }
    }
    Some(result)
  }
  else None

  def children: Seq[PathArtifact[T]]
}

object ExternalPathArtifact {
  def apply(s: ExternalPath): ExternalPathArtifact = new MemoryExternalPathArtifact(s)
}

trait ExternalPathArtifact extends PathArtifact[ExternalPath] {
  //} with ContentHashableArtifact[ExternalPath] {

  // todo think about preemptive hashing!
  override lazy val constantId = Identifier[Artifact[ExternalPath]]("ExternalPath(" + abspath + ")")

  override def toString = abspath.toString

  override lazy val children: Seq[ExternalPathArtifact] = {
    if (path.nonExistent || path.isFile) Nil
    else path.children().toSeq.filter(p => {
      !WorldMakeConfig.ignoreFilenames.contains(p.name)
    }).sorted.map(f => new MemoryExternalPathArtifact(ExternalPath(f)))
  }
}

object ManagedPathArtifact {
  def apply(s: ManagedPath): ManagedPathArtifact = new MemoryManagedPathArtifact(s) //tap Storage.provenanceStore.put
}

trait ManagedPathArtifact extends PathArtifact[ManagedPath] {
  // todo think about preemptive hashing!
  override lazy val constantId = Identifier[Artifact[ManagedPath]]("ManagedPath(" + value + ")")

  override def toString = value.id.toString

  override lazy val children: Seq[ManagedPathArtifact] = {
    if (path.nonExistent || path.isFile) Nil
    else path.children().toSeq.filter(p => {
      !WorldMakeConfig.ignoreFilenames.contains(p.name)
    }).sorted.map(f => new MemoryManagedPathArtifact(value.child(f.name))) //ManagedPath(value.id, f))
  }
}

case class TypedExternalPathArtifact(value: ExternalPath) extends ExternalPathArtifact

case class TypedManagedPathArtifact(value: ManagedPath) extends ManagedPathArtifact

class MemoryExternalPathArtifact(ep: ExternalPath) extends MemoryArtifact[ExternalPath](ep) with ExternalPathArtifact with Logging {
  // ** require(path.exists)

  if (!ep.path.exists) {
    logger.warn("External path artifact does not exist: " + path)
  }

  require(!path.toAbsolute.path.endsWith(".hg"))
  //   If it's a directory, this should in some sense include all the files in it (maybe just tgz?)-- but be careful about ignoring irrelevant metadata.
  /*override protected def bytesForContentHash = if (path.isFile) new FileInputStream(path.fileOption.get)
  else {
    path.children()
  }*/

  /*
  override def /(s: String): ExternalPathArtifact = new MemoryExternalPathArtifact(path / s)
 */

  //lazy val output: Option[ExternalPathArtifact] = Some(this)

  //def pathType = classManifest[T].toString
}


class MemoryManagedPathArtifact(pathid: ManagedPath) extends MemoryArtifact[ManagedPath](pathid) with ManagedPathArtifact with Logging {
  //} with ContentHashableArtifact[ManagedPath] {

  if (!pathid.path.exists) {
    logger.warn("Managed path artifact does not exist: " + path)
  }

  //lazy val output: Option[ManagedPathArtifact] = Some(this)

}

trait TypedPathCompanion {
  def mapper: PathReference => TypedPathReference

  private def wrapper[T <: TypedPathReference : ClassManifest]: (Recipe[PathReference]) => TypedPathRecipe[T] = RecipeWrapper.makePathRecipeTyped[T](p => mapper(p).asInstanceOf[T])

  //implicit
  def wrapRecipe[T <: TypedPathReference : ClassManifest](d: Recipe[PathReference]): TypedPathRecipe[T] = {
    val w = wrapper[T]
    w(d)
  }

  //def wrapper: (Derivation[Path]) => TypedPathDerivation[T] = DerivationWrapper.wrapDerivation[T](mapper)
}


/*
object TypedPathMapper {
  /*val typeMappings : mutable.Map[String, Path=>TypedPath] = mutable.HashMap[String, Path=>TypedPath]()
   
  def register[T<:TypedPathCompanion:ClassManifest](t:T) {
    val toType = classManifest[T].getClass.getName
    typeMappings.put(toType,t.mapper)
  }*/
 /* 
  def register(pathType:String, mapper:Path=>TypedPath) = {
    typeMappings.put(pathType,mapper)
  }
*/
  //def map[T <: TypedPath](pathType:String,file:Path) : T = typeMappings(pathType)(file).asInstanceOf[T]


  import scala.reflect.runtime.{universe => ru}
  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  def map[T <: TypedPath](pathType:String,file:Path) :T = {
    val clz = Class.forName(pathType)
    val classSymbol = mirror.classSymbol(clz)
    val cType = classSymbol.toType
    val cm = mirror.reflectClass(classSymbol)
    val ctorC = cType.declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctorC)

    // the cast asserts that the file type requested via the "pathType" string was actually mapped to the right type
    ctorm(file).asInstanceOf[T]
    
    //val const = Class.forName("pathType").getConstructor(Class[Path])
  }

}
*/
