---
layout: default
install: other
---


Security
========

In the fully distributed model, WorldMake may automatically download derivations from remote servers and then execute them.  Consequently it is a possible vector for malevolent code to run on your machine.  This is of course no different from what most of us do many times a day: compiling and running code acquired from GitHub, or from Maven repositories, or using package managers such as apt-get, and so on and so forth.  Nonetheless we plan to introduce a trust model in a future version; but for now it's worth being aware of the risks.

Derivation isolation
====================

A recipe should not use any inputs (including programs and libraries) which are not explicitly specified.  We could enforce this by running the derivations in a chroot jail (similar to Nix), and perhaps somehow even blocking network access.  At present we don't go to such extreme measures, so it's an honor system thing not to write impure recipes.  A tiny amount of protection can be provided by limiting the PATH in the shells in which the derivations run (via the `globalpath` option), but then you will have to explicitly pass in any programs you might need (e.g. from `/usr/local/bin`) in the form of Recipe prerequisites.

Deterministic and Nondeterministic Derivations
==============================================

**TODO**

Type safety
===========

One might be concerned that a `Recipe[Path]` could produce any file or directory, and we'd have no idea what to expect to find there.  To address this, WorldMake allows establishing a mapping from file types to Scala types, to take full advantage of Scala's strong type system.  The concept may apply to standard file types (e.g. text files, tar/gzip archives, etc.) and to "types" of directories, which may be required to contain certain files or subdirectories (which may themselves be typed).

This mechanism can be used to provide *compile-time* checks, to guarantee that we don't accidentally pass a binary file to a program that requires a text input, or that the input to a program that expects to find a file under "input/foo/bar/baz.gz" can only be wired to the output of another program that is known to write such a file in its output.

**TODO: explain details** 

Execution strategies
====================

**TODO**