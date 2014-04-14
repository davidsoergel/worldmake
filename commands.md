---
layout: default
commands: active
---

It is safe to run multiple `wm` processes simultaneously; in particular, you can use `wm queue` in a second window to monitor progress of a `wm make` run.

Global Commands
===============

**wm clean**: Remove any provenances that are blocked, pending, failed, cancelled, or running.  (This does not kill qsub jobs, though, so you may need to do that manually in the event of a crashed or aborted run).

**wm gc**: Garbage-collect metadata, file, and log caches.


Recipe Commands
===============

**wm make &lt;target&gt;**: Build the named target and report the result.

**wm show &lt;target&gt;**: Display some information about the target.

**wm deps &lt;target&gt;**: Show immediate dependencies of the target.

**wm queue &lt;target&gt;**: Show the partial-order execution queue for the target (including job status).


Provenance Commands
===================

**wm show &lt;provenance ID&gt;**: Display some information about the Provenance.

**wm deps &lt;provenance ID&gt;**: Show immediate dependencies of the Provenance.

**wm queue &lt;provenance ID&gt;**: Show the partial-order execution queue for the Provenance (including job status).

**wm log &lt;provenance ID&gt;**: Show head and tail of a run log

**wm logfull &lt;provenance ID&gt;**: Show full log of a run