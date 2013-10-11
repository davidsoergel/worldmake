package worldmake

import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


/**
 * A World is a Recipe factory.
 */
trait World extends (String=>Recipe[_]) {
  def as[T](s:String) : Recipe[T] = apply(s).asInstanceOf[Recipe[T]]
}

/**
 * A ConcreteWorld in prepopulated with named Derivations.
 * @param targets
 */
class ConcreteWorld (targets: Map[String, Recipe[_]] ) extends World {
  def apply(name:String) = targets(name) //.getOrElse(Storage.pr)
  def get(name:String) = targets.get(name)
}


/**
 * A ConstantWorld in prepopulated with named Constant derivations, as e.g. loaded from a properties file.
 */
class ConstantWorld (ts: Map[String, ConstantRecipe[_]] ) extends ConcreteWorld(ts)

class PathWorld (ts: Map[String, Recipe[Path]] ) extends ConcreteWorld(ts) {
  override def apply(name:String) = ts(name)
  override def get(name:String) = ts.get(name)
}

trait RecipeFactory extends ((World) => Recipe[_])

trait WorldExpander extends ((World)=>World)


trait WorldFactory {
  def get : World
}
