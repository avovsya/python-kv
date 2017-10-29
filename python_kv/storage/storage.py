# SSTable operations:
#   1 Create binary sstable
#   2 Write binary representation to a file
#   3 Create binary representation of the index
#   4 Write binary index to a file
#   5 Create in memory index from Memtable
#   6 Create in memory index from binary file
#   7 Get item from a file using offset from an index
# TODO: compaction, bloom filters, memtable -> sstable in a separate thread, compaction in separate thread, btc...

# Entities:
# 1. * Memtable - 1, 3, 5
# 2. * SSTableIndex - 6, can load itself from a file
# SSTable - 2, 4, 7 + class is a wrapper for previous 2
# Only SSTable class is used by Storage class
from python_kv.storage.memtable import Memtable
from python_kv.storage.sstable_group import SSTableGroup


class Storage():
    def __init__(self, db_path):
        self._current_memtable = Memtable()
        self._sstable_group = SSTableGroup.from_directory(db_path)
        pass


    def put(self, key, value):
        self._current_memtable.put(key, value)
        # TODO: Check memtable size
        # If it's smaller than THRESHOLD:
        #   Add key to memtable
        # Otherwise:
        #   Write memtable to the new SSTable on disk(table + table index)
        #   Load that SSTable's index into memory
        #   Create new memtable
        #   Add key to the new memtable


    def delete(self, key):
        # TODO: Same as put
        self._current_memtable.delete(key)


    def get(self, key):
        result = self._current_memtable.get(key)
        if result is not None:
            return result.get_value()

        result = self._sstable_group.get(key)

        if result is not None:
            return result.get_value()

        return None
