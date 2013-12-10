---
layout: default
about: active
---

<!--- Distributed, Lazy, Pure Functional Build Tool for Reproducible Research -->

WorldMake is a system for **executing computational workflows**, **tracking provenance**, and **keeping derived results up to date**.  It can be used for any situation involving a network of computations that depend on one another, such as:

* software compilation
* software package management
* continuous integration and testing
* reproducible research.

*WorldMake is presently alpha-level research software.  Consequently some of the below are "forward looking statements" describing features which are not yet fully fleshed out.  We are however confident in the model and the basic functionality.*

---

Like the venerable `make` tool, WorldMake guarantees that a collection of derived results is up-to-date and internally consistent, while avoiding redundant computation.  But it provides much more:

* Recipes (i.e., steps of a workflow) are easily **shareable** through distributed version control (e.g., GitHub).
* Recipes can be **easily composed** from other recipes obtained from different repositories.
* Consequently, workflow specifications can be **distributed among collaborators**-- i.e., if my collaborator updates an input file, I can automatically recompute dependent results.

* The stateless, pure functional nature of workflows guarantees **reproducibility** of results.
* SHA-256 hashing throughout provides for automatic **integrity verification** of results.
* Rigorous **provenance tracking**, including timing information and log outputs.
* Storage of **historical results**, for easy comparison with the latest version of a given output *(subject to a garbage collection policy, to keep storage requirements in check)*.
* **Archival** of the complete provenance of any artifact, including all prerequisite inputs, intermediate results, and derivation programs.
* Automatic **concurrent execution** of independent computations, both on single **multicore machines** and on **SGE clusters**.
* Workflows are specified in **Scala**, providing all the flexibility of a **real programming language** (as opposed to highly constrained and frankly wonky dependency languages such as `make`).  Aside from the syntactic improvement, this means that workflows can be structured like any other program, enjoying all the benefits of object orientation, inheritance, packaging, information hiding, and so forth.
* It is trivial to specify **parameter sweeps**, as well as to gather and filter sweep results for plotting or downstream analysis.
* Workflow computations are **strongly typed**, facilitating both compile-time and runtime checking that the output of one recipe is a valid input for the next.  In fact, authoring workflows in a Scala-type-aware IDE such as IntelliJ IDEA provides instantaneous feedback on type compatibility.

Meaning of the name "WorldMake"
===============================

1.  While not technically related to `make` in any way, WorldMake is in some ways its conceptual descendant, and provides a superset of its functionality.
2.  WorldMake recipes, inputs, intermediate results, and outputs can be publicly shared and composed with full tracking of provenance.  This means that if someone has already done the computation that you want to do and is sharing the result, you can just download that output with full confidence that it is the same result you would have computed.  Thus, in a sense, all users are participating in *a single worldwide build system*.
3.  All outputs of a workflow ultimately derive from a set of concrete, non-derived inputs such as raw data files and source code (both of the workflow components and of the workflow itself).  These inputs are axioms, and together they define a World of potential derivations--which must of course be internally consistent.  So, the name is meant to evoke this concept of a *consistent container*.  If you disagree with someone about an axiom, then you are living in a different World, so naturally your results may not agree with theirs. 
