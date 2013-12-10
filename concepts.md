---
layout: default
concepts: active
---

Concepts
========

Recipes
-------

A recipe can be thought of simply as a pure function from some set of inputs to some output.  However: the code that specifies the recipe is not just the function itself, because we don't want it to simply execute; rather, we want wrap the function in such a way that it executes only after its dependencies are ready.  Further, we want the recipe to be executed only when it is necessary to do so-- not, for instance, if a valid result is already cached.  A Recipe is therefore a specification of a computation that may or may not need to be executed at a later time.  In order to "cook" the recipe, WorldMake must first acquire the ingredients (perhaps by cooking other Recipes), and must then perform the specified operations on them.

A recipe may be cooked many times, each time perhaps with the same ingredients or perhaps with ingredients acquired from different sources.

A recipe is uniquely identified by the SHA-256 hash of its textual representation, combined with the hashes of Recipes on which it depends.  Thus, when an change is made to an Recipe, all Recipes that depend on it are considered to have changed as well.


Provenances
-----------

A Provenance is is the combination of a recipe with a description of exactly which ingredients are to be used (or were used) when cooking it.  That is: while a Recipe calls for ingredients in the abstract ("Spaghetti Sauce"), a Provenance states concretely where those ingredients must come from ("Spaghetti sauce from the jar in the refrigerator-- the one bought from Safeway on July 23").  These ingredient specifications are themselves Provenances: that is, they may not yet be on hand, but it is known exactly where they will come from.

Before a recipe is cooked, the Provenance provides a concrete specification of what is to be done.  After the recipe is cooked, the Provenance remains unchanged, and provides a record of exactly what was done.

Provenances have a lifecycle, traversing states as follows:

* Blocked: the upstream Provenances are not yet successfully completed.
* Pending: the upstream Provenances are ready, but the recipe has not yet been cooked.
* Running: cooking is underway.
* Failed: an error occured in the derivation.
* Success: the recipe was cooked successfully, resulting in an Artifact.
* Constant: a successful Provenance whose output Artifact value is hard-coded.

In the course of tracking the lifecycle, Provenances also record metadata about the execution, such as the times at which lifecycle transitions occurred, standard output and error logs, information about which cluster node was used for the computation, and so forth.

Provenances are uniquely identified by UUID.

Artifacts
---------

An Artifact is the concrete output of a Provanance.  In the typical "make"-like usage, this will be a Path, representing a file or a directory structure containing multiple files.  It may also be a simple data type within the JVM, such as an Boolean, Int, Double, String, or a list of other Artifacts of known type.  

More complex Scala types are not directly supported, because the need to serialize and deserialize these to Mongo or the filesystem would introduce too much complexity and potential brittleness.  

Artifacts do not require their own unique IDs, because they are always wrapped in a Provenance; in our model, the Provenance is the only sensible way to refer to an Artifact anyway.  Artifacts do however have SHA-256 hashes computed from their full contents, to facilitate integrity verification and deduplication.

