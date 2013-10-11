Worldmake is a system for executing computational workflows, tracking provenance, and keeping derived results up to date.  It can be used for

* reproducible research
* software compilation

Like the venerable "make" tool, WorldMake guarantees that a collection of derived results are up-to-date and internally consistent.  But it provides much more:

Open source
Precisely specify dependencies
Keep results up-to-date 
Keep results internally consistent
Avoid redundant computation
Manage cluster job distribution (shared FS)
Manage cluster job distribution (staging)
Track provenance
Facilitate archival
Store historical derived results (and GC)
Composable
Shareable
Distributed (among collaborators)
Parametrizable
Parameter sweeps
Gather & filter sweep results for plotting etc.
Stateless, Pure Functional
Strongly typed




Security
========

In the fully distributed model, WorldMake may automatically download derivations from remote servers and then execute them.  Consequently it is a possible vector for malevolent code to run on your machine.  This is of course no different from what most of us do many times a day: compiling and running code acquired from GitHub, or from Maven repositories, or using package managers such as apt-get, and so on and so forth.  Nonetheless we plan to introduce a trust model in a future version; but for now it's worth being aware of the risks.


Collaboration
=============

Multiple users on the same machine may share a WorldMake database; all that is required is to set the access permissions appropriately for the Mongo database and for the managed files.  (It may be necessary to set the group sticky bit 

Prerequisites
=============

* MongoDB
* JDK 1.7

For cluster usage: 
* SGE (i.e., "qsub"), with a filesystem shared among the head and compute nodes.  (Support for operation without a shared filesystem is planned for a future version).

Installation and configuration
==============================

WorldMake is distributed as a single jar file, together with an executable shell script for setting default command-line parameters.

WorldMake tracks workflow inputs, intermediate results, and outputs in two forms: Metadata and small artifacts are stored in a Mongo database, and larger files are stored in a managed directory tree in the filesystem.  Log files are similarly stored either in the database or in the filesystem, depending on size.  The root of the worl

Derivations are performed in a temporary working directory, where they may write ephemeral files which are not considered part of the output.

WorldMake looks for a configuration file in ~/.worldmake where the loca


Derivation isolation
====================

Ideally it should not be the case that a recipe uses any input which is not explicitly specified.  We could enforce this by running the derivations in a chroot jail (similar to Nix), and perhaps somehow even blocking network access.  At present we don't go to such extreme measures, so it's an honor system thing not to write impure recipes.  A tiny amount of protection is provided by the fact that the system path is limited in the shells in which the derivations run.


Concepts
========

Recipes
-------

A recipe can be thought of simply as a pure function from some set of inputs to some output.  However: the code that specifies the recipe is not just the function itself, because we don't want it to simply execute; rather, we want wrap the function in such a way that it executes only after its dependencies are ready.  Further, we want the recipe to execute only if it is required to do so-- not, for instance, if a valid result is already cached.  A Recipe is therefore a specification of a computation that may or may not need to be executed at a later time.  In order to "cook" the recipe, WorldMake must first acquire the ingredients (perhaps by cooking other Recipes), and must then perform the specified operations on them.

A recipe may be cooked many times, each time perhaps with the same ingredients or perhaps with ingredients acquired from a different source.


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

In the course of tracking the lifecycle, Provenances also record metadata about the execution, such as the times at which lifecycle transitions occurred, standard output and error logs, information about which cluster node was used for the computation, and so forth.


Artifacts
---------

An Artifact is the concrete output of a Provanance.  In the typical "make"-like usage, this will be a Path, representing a file or a directory structure containing multiple files.  It may also be a simple data type within the JVM, such as an Int, Double, or String (or arrays of these?).

More complex Scala types are not directly supported, because the need to serialize and deserialize these to Mongo or the filesystem would introduce too much complexity and potential brittleness.  


How to write a recipe
=====================

Thus, a 


Type safety
===========


Execution strategies
====================
