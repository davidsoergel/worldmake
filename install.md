---
layout: default
install: active
---

Prerequisites
=============

* MongoDB
* JDK 1.7

For cluster usage: 

* SGE (i.e., "qsub"), with a filesystem shared among the head and compute nodes.  *(Support for operation without a shared filesystem is planned for a future version).*

Installation and configuration
==============================

WorldMake is distributed as a single jar file, together with an executable shell script "wm" for setting default command-line parameters.  Just put these in some convenient place, accessible in your path.

Next create the file ~/.worldmake/application.conf, and populate it with configuration options as follows.

Storage configuration
---------------------

WorldMake tracks workflow inputs, intermediate results, and outputs in two forms: metadata and small artifacts are stored in a Mongo database, and larger files are stored in a managed directory tree in the filesystem.  Log files are similarly stored either in the database or in the filesystem, depending on size.

Various filesystem paths below are independently configurable, but it's probably easiest to puth tem all together in some convenient place, e.g. `~/worldmake` for personal usage or `/somewhere/worldmake` for shared usage.  Just be sure that there's a lot of space available at that location!

    # MongoDB connection information
    mongoHost = mongoserver.mydomain.com
    mongoDbName = worldmake
    
    # Filesystem locations for primary artifact storage and log storage
    filestore = /somewhere/worldmake/file
    logstore = /somewhere/worldmake/log
 
Derivations are performed in a temporary working directory, where they may write ephemeral files which are not considered part of the output.
    
    # location for working directories
    localTempDir = /tmp/worldmake

VCS integration
---------------

    # the root path of a remote Git server containing your repos
    gitremote = "file:///Git_Repos/worldmake/"
    
    # A local path where Git clones can be kept
    gitlocal = /somewhere/worldmake/gitlocal/
    
    # the root path of a remote Mercurial server containing your repos
    hgremote = "file:///somewhere/worldmake/hgcanonical/"
    
    # A local path where Mercurial clones can be kept
    hglocal = /somewhere/worldmake/hglocal/

SGE options
-----------

At present, SGE usage requires the `filestore` and `logstore` options above to be on a shared filesystem.

    qsub = /opt/sge-6.2u5/bin/lx24-amd64/qsub
    qstat = /opt/sge-6.2u5/bin/lx24-amd64/qstat
    
    # this directory must be on a shared filesystem.
    qsubGlobalTempDir = /emerge/umass/worldmake/qsubTmp
    
Execution options
-----------------

    # working directories are deleted by default when a provenance succeeds; however it may be useful to retain them for debugging.
    # working directories of failed provenances are retained, regardless, until the provenance itself is garbage-collected.
    debugWorkingDirectories = true
    
    # this PATH will be available within system derivations.  (This constitutes a leak in purity and so is distasteful, but it's expedient for now).
    globalpath = "/sw/bin:/sw/sbin:/Developer/usr/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/sbin:/usr/bin:/bin:/opt/X11/bin:/opt/local/bin:/opt/git-1.7.4.1-x86_64/bin:/usr/java/jdk1.7.0_21/bin"
    
    # When computing the hash of a Path artifact, ignore these filenames.  Include anything here that is ephemeral and/or meaningless as far as the derivation is concerned, so that irrelevant changes don't break artifact identity.
    ignoreFilenames = [ ".hg", ".git", ".svn" ]
    
    # Keep trying to compute failed derivations.  It's probably best to leave this false, so that you can fix the cause of the failure.  A value fo true only makes sense if your derivations may exhibit transient errors that you prefer to ignore.
    retryFailures = false 
    
    # Method of executing system derivations (i.e., snippets of shell script).
    # Options: "local", "qsub"
    executor = qsub


Collaboration
=============

Multiple users on the same machine may share a WorldMake database; all that is required is to set the access permissions appropriately for the Mongo database and for the managed file paths.  It may be necessary to set the group sticky bit at the root of the worldmake tree, e.g. `/somewhere/worldmake`, so that new files created by one collaborator will be available to others.

It's fine for multiple instances of the WorldMake program to run simultaneously against the same database and file stores.

It's fine for multiple instances of the WorldMake program to run simultaneously against the same database and file stores.
