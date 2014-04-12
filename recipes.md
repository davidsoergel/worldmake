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


Interpolated String Derivations
--------------------------------

The Scala `s` [string interpolator](http://docs.scala-lang.org/overviews/core/string-interpolation.html) is a convenient way to compose new Strings from other Strings.  Worldmake leverages this mechanism, providing a custom `ds` interpolator to construct a new `Recipe[String]` from a set of other `Recipe`s.

```scala
val inputDirectory : Recipe[String] = ...
val year : Recipe[Int] = ...
val inputFileName : Recipe[String] = ds"""${inputDirectory}/${year}.gz"""
```

To be clear: this creates a *lazy* interpolation.  When the `Recipe` is cooked, the interpolated `Recipe`s must also be cooked; their results are then interpolated to produce the result (see [Concepts](/concepts.html)).


External Paths
--------------

```scala
val inputFileName : Recipe[String] = ...
val inputPath: TypedPathRecipe[BasicTextFile] = 
  RecipeWrapper.externalPathFromString(inputFileName);
```


System Derivations
------------------

A *System Derivation* is a `Recipe[Path]` that can be easily specified using the custom `sys` string interpolator.  Like a `ds` interpolation, the interpolated variables must be upstream `Recipe`s.  The interpolated string forms a shell script which is executed in a temporary working directory.  An environment variable named `out` is provided, pointing to a unique filesystem path where output may be written.  This path is the result of cooking the recipe.

A common simple case is to construct a single command line, as follows:

```scala
  lazy val myProgramOutput : Recipe[Path] = sys"""
    | ${myProgram} --foo=${theFooArgument} ${barCount} > $${out}
    """
```

Note that the single `$` indicates a *Scala* variable containing a `Recipe` to be interpolated, whereas the double `$$` is an escape sequence allowing the generated shell script to contain `${out}`, so that the *shell* variable by that name can be used at runtime.

The `out` path has not yet been created when the script executes.  Thus, the script may choose to write a file directly to that path, or to create a directory there and write outputs within it.

A slightly more elaborate example:

```scala
lazy val myProgramOutput : Recipe[Path] = sys"""
  | mkdir -p $${out}
  | sort -k2,2 -n -r ${in} > sorted
  | ${myFooProgram} sorted > $${out}/foo.output
  | ${myBarProgram} sorted > $${out}/bar.output
  """
```

Note that in this case the "sorted" file is not retained; it existed only in the temporary working directory.  The output is a Path that will always contain both "foo.output" and "bar.output".


Sequences (really, GenTraversables)
-----------------------------------

```scala
import worldmake.TraversableRecipe._
```

provides an implicit conversion from `GenTraversable[Recipe[T]]` to `Recipe[GenTraversable[Artifact[T]]]`.  Thus, if you write a recipe that requires a list of inputs, you can feed a list of *Recipes* for those inputs, which will be executed *concurrently* before the list is assembled from the outputs.


Assemblies
----------

An Assembly is a Recipe[Path] which produces a directory containing symlinks to path artifacts generated from other recipes.


Recipe Descriptions
-------------------

Any `Recipe` may be annotated with a textual description for use in progress reports and logs.  This is done using the `via` operator:

```scala
  lazy val myProgramOutput = "Frobbify the shiznitz" via sys"""
    | ${myProgram} --foo=${theFooArgument} ${barCount} > $${out}
    """
```