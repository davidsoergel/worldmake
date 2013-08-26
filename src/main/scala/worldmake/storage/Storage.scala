package worldmake.storage

import worldmake.storage.ProvenanceStore

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object Storage extends StorageContext {
  // default this to memory based storage
  var _storage: StorageContext = _

  def setStorage(s: StorageContext) {
    _storage = s
  }

 // def artifactStore: ArtifactStore = _storage.artifactStore
  def provenanceStore: ProvenanceStore = _storage.provenanceStore

}


trait StorageContext {
  //def artifactStore: ArtifactStore
  def provenanceStore : ProvenanceStore
}


// this exposes setStorage but not other methods (Storage itself is private[model])
object StorageSetter {
  def apply(s: StorageContext) {
    Storage.setStorage(s)
  }
}
