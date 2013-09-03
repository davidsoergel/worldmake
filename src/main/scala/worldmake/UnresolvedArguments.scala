package worldmake

import scala.collection.{GenMap, GenSet}
import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent._

/** Go through some contortions to establish type safety between Derivations and Provenances, while abstracting out the DerivationStrategy.
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
sealed trait UnresolvedArguments {
  def derivations: GenSet[Derivation[_]]
  def queue: Queue[Derivation[_]]
  //def resolve(strategy:DerivationStrategy) : ResolvedArguments
  //def resolve(strategy:FutureDerivationStrategy) : Future[ResolvedArguments]
}

class UnresolvedArguments0 extends UnresolvedArguments {
  def derivations= Set.empty
  def queue = Queue.empty
  //def resolve(strategy:DerivationStrategy) : ResolvedArguments0 = new ResolvedArguments0 //= strategy.resolveOne()
  //def resolve(strategy:FutureDerivationStrategy) : Future[ResolvedArguments0] = future { new ResolvedArguments0 }
}

class UnresolvedArguments1[A](val a: Derivation[A]) extends UnresolvedArguments {
  def derivations= Set(a)
  def queue = a.queue
  //def resolve(strategy:DerivationStrategy) = new ResolvedArguments1(strategy.resolve(this))
  //def resolve(strategy:FutureDerivationStrategy) = strategy.resolveOneFuture(a) map {new ResolvedArguments1(_)} 
}

class UnresolvedArguments2[A, B](a: Derivation[A], b: Derivation[B]) extends UnresolvedArguments {
  def derivations= Set(a,b)
  def queue = (a.queue ++ b.queue).distinct
  //def resolve(strategy:DerivationStrategy) = new ResolvedArguments2(strategy.resolveOne(a))
  //def resolve(strategy:FutureDerivationStrategy) = strategy.resolveOneFuture(a) map {new ResolvedArguments1(_)}
}


class UnresolvedArguments3[A, B, C](a: Derivation[A], b: Derivation[B], c: Derivation[C]) extends UnresolvedArguments {
  def derivations= Set(a,b,c)
  def queue = (a.queue ++ b.queue ++ c.queue).distinct
}


class UnresolvedArgumentsSet[A](val values: GenSet[Derivation[A]]) extends UnresolvedArguments {
  def derivations= values
  def queue = Queue(values.toSeq.seq.flatMap(_.queue): _*).distinct
}

class UnresolvedArgumentsNamed[A](map: GenMap[String, Derivation[A]]) extends UnresolvedArguments {
  def derivations = map.values.toSet
  def queue = Queue(map.values.toSeq.seq.flatMap(_.queue): _*)
}

trait ResolvedArguments

class ResolvedArguments0 extends ResolvedArguments

class ResolvedArguments1[A](a: Successful[A]) extends ResolvedArguments

class ResolvedArguments2[A, B](a: Successful[A], b: Successful[B]) extends ResolvedArguments


class ResolvedArguments3[A, B, C](a: Successful[A], b: Successful[B], c: Successful[C]) extends ResolvedArguments

class ResolvedArgumentsSet[A](val values: GenSet[Successful[A]]) extends ResolvedArguments


class ResolvedArgumentsNamed[A](map: GenMap[String, Successful[A]]) extends ResolvedArguments

/*
trait Arguments {
  def unresolved  : UnresolvedArguments
  def resolved(strategy:DerivationStrategy) : ResolvedArguments
  def resolvedFuture(strategy:FutureDerivationStrategy) : FutureResolvedArguments
}

class Arguments1[A](u:UnresolvedArguments1[A]) extends Arguments{
  def unresolved : UnresolvedArguments1[A] = u
  def resolved(strategy:DerivationStrategy) : ResolvedArguments1[A] = strategy.resolve(unresolved)
}


class ArgumentsSet[A](u:UnresolvedArgumentsSet[A]) extends Arguments{
  def unresolved : UnresolvedArgumentsSet[A] = u
  def resolved(strategy:DerivationStrategy) : ResolvedArgumentsSet[A] = strategy.resolve(unresolved)
}
*/
