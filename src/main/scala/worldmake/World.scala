package worldmake

import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


/**
 * A World is a Derivation factory.
 */
trait World extends (String=>Derivation[_]) {
  def as[T](s:String) : Derivation[T] = apply(s).asInstanceOf[Derivation[T]]
}

/**
 * A ConcreteWorld in prepopulated with named Derivations.
 * @param targets
 */
class ConcreteWorld (targets: Map[String, Derivation[_]] ) extends World {
  def apply(name:String) = targets(name)
  def get(name:String) = targets.get(name)
}


/**
 * A ConstantWorld in prepopulated with named Constant derivations, as e.g. loaded from a properties file.
 */
class ConstantWorld (ts: Map[String, ConstantDerivation[_]] ) extends ConcreteWorld(ts)

class PathWorld (ts: Map[String, Derivation[TypedPath]] ) extends ConcreteWorld(ts) {
  override def apply(name:String) = ts(name)
  override def get(name:String) = ts.get(name)
}

trait DerivationFactory extends ((World) => Derivation[_])

trait WorldExpander extends ((World)=>World)


trait WorldFactory {
  def get : World
}
