---
layout: default
worlds: active
---

More on Worlds
==============

Worlds as maps from names to Recipes
------------------------------------

We mentioned previously that a World is a container for things that are consistent with one another.  It is also a mapping from unique names to Recipes.  There may be far more Recipes present (or implied) in the World than are named; the names serve as the means of publicly exposing a Recipe outside of the World.  That is:

```scala
trait World extends (String=>Recipe[_])
```

Names can be any strings convenient for you, since in general they're not propagated to remote users.

The most important Recipes to name are those specifying the final outputs of your pipeline (aka build targets), because you can provide these names on the command line to request a particular computation.  For example, given this World:

```scala
 new World {
      def apply(target: String) = target match {
        case "soergel.papers.2013.proofOfPEqualNP.figure4" => Figure4.plot
      }
    }
```

Then to generate the plot (or just get a reference to the previously generated plot, if nothing has changed), you can then write

```bash
wm make soergel.papers.2013.proofOfPEqualNP.figure4
```

The result will be to cook Figure4.plot (a Recipe[Path]), providing the resulting Path (in this case, presumably, an image file).

Expanding Worlds
----------------

You can easily define a World that expands on another World by adding more named Recipes.  This is done via a WorldExpander:

```scala
trait WorldExpander extends ((World)=>World)
```

that simply adds new Recipes to an existing World.  For instance:

```scala

object FoobarWorldExpander extends WorldExpander {
  def apply(underlyingWorld: World) = {
    val foo : Recipe[Path] = ...
    val bar : Recipe[Path] = ... underlyingWorld("baz") ...
 new World {
      def apply(target: String) = target match {
        case "foo" => foo
        case "bar" => bar
        case x => underlyingWorld(x)
      }
    }
```

where the `foo` and `bar` Recipes may take prerequisites from the underlying World.

In a complex application, it may well be a good design to place all of your axioms (i.e., constant inputs) in a World of their own, and then to specify derivations through a WorldExpander.  The advantage is that you can cleanly express that the same set of derivations may be applied to different versions of the axioms.

Worlds obtained from version control
------------------------------------

A special kind of World is one obtained by checking out a set of repositories from Git or Mercurial.  This is achieved through the provided VcsWorld, which can be used as follows:

```scala

object BazQuxWorldFactory extends WorldFactory {
  lazy val workspaces = GitWorkspaces  // or MercurialWorkspaces

  lazy val reposRequestedVersions: Map[String, (String, String)] = Map(
    "baz" -> ((workspaces.defaultBranchName, "latest")),
    "qux" -> ((workspaces.defaultBranchName, "latest"))
    )

  lazy val gworld = (new VcsWorldMaker(workspaces))(reposRequestedVersions)

  lazy val get = FoobarWorldExpander(gworld)
}
```

Here, the World `gworld` maps each repository name to a Path containing a clean working copy checked out from that repo.  We could request specific versions from the `baz` and `qux` repositories (by commit ID), but in this case, we request the "latest" version of each.  The consequence is that the current commit id of the HEAD is considered a constant string input, and so is recorded in the WorldMake database; the working copy (a Path) is then considered a result derived from that commit ID.  Once the working copies are in hand, the WorldExpander can operate on them to provide the downstream derivations.

A consequence of this setup is that it is possible to simply push a change to some repository, say `baz`, and then rerun `wm make bar`.  Because the commit ID at the head of the baz repository has changed, any downstream derivations are invalidated and must be recomputed.
