---
layout: default
recipes: active
---

How to write a recipe
=====================

Constants
---------

All axioms are provided as ConstantRecipes.  The minimum WorldMake workflow defines one constant-- perhaps the name of a VCS repository-- and then performs some action on it (e.g., checking out a working directory from the specified repo and running `mvn compile`).

Constants may be Boolean, Int, Double, String, or Path.  Implicit conversions to these are provided via

```scala
import worldmake.ConstantRecipe._
```

So that you can write

```scala
val gravitationalConstant : Recipe[Double] = 9.81
```


String interpolation
--------------------

System derivations
------------------

Sequences (really, GenTraversables)
-----------------------------------

```scala
    import worldbake.TraversableRecipe._
```

provides an implicit conversion from `GenTraversable[Recipe[T]]` to `Recipe[GenTraversable[Artifact[T]]]`.  Thus, if you write a recipe that requires a list of inputs, you can feed a list of *Recipes* for those inputs, which will be excuted concurrently before the list is assembled from the outputs.

Assemblies
----------

An Assembly is a Recipe[Path] which produces a directory containing symlinks to path artifacts generated from other recipes.

