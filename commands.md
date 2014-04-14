---
layout: default
commands: active
---

Global Commands
===============

wm clean
: Remove any provenances that are blocked, pending, failed, cancelled, or running.  (This does not kill qsub jobs, though, so you may need to do that manually in the event of a crashed or aborted run).

wm gc
: Garbage-collect metadata, file, and log caches.


Recipe Commands
===============

wm make
: Build the named target and report the result.

wm show <target>
: Display some information about the target.

wm deps <target>
: Show immediate dependencies of the target.

wm queue <target>
: Show the partial-order execution queue for the target (including job status).


Provenance Commands
===================

wm show <provenance ID>
: Display some information about the Provenance.

wm deps <provenance ID>
: Show immediate dependencies of the Provenance.

wm queue <provenance ID>
: Show the partial-order execution queue for the Provenance (including job status).

wm log <provenance ID>
: Show head and tail of a run log

wm logfull <provenance ID>
: Show full log of a run