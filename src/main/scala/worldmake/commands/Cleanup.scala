package worldmake.commands

import worldmake.storage.Storage

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object Cleanup {
  
  def gcdb(): String = {
    while(Storage.provenanceStore.removeUnused) {}
    "done cleaning metadata"
  }

  def gcfiles(): String = {
    val provs = Storage.provenanceStore
    // perf
    for((id, p) <- Storage.fileStore.all) {
      if(provs.findWithArtifactFileById(id).isEmpty) if(p.isFile) p.delete() else p.deleteRecursively()
    }
    Storage.fileStore.cleanup()
    "done cleaning files"
  }

  def gclogs(): String = {
    val provs = Storage.provenanceStore
    // perf
    for((id, p) <- Storage.logStore.all) {
      if(provs.findWithLogFileById(id).isEmpty) p.delete()
    }
    "done cleaning logs"
  }
}

