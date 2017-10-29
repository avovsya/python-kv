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

class Storage():
    def __init__(self):
        # Initialize memtable
        # Read existing SSTables from disk
        # Load SSTables indices into memory
        pass

    def put(self, key, value):
        # Check memtable size
        # If it's smaller than THRESHOLD:
        #   Add key to memtable
        # Otherwise:
        #   Write memtable to the new SSTable on disk(table + table index)
        #   Load that SSTable's index into memory
        #   Create new memtable
        #   Add key to the new memtable
        pass


    def delete(self, key):
        # Same as put
        pass


    def get(self, key):
        # Check memtable for a key
        # If key is in memtable:
        #   Return value from memtable
        # Otherwise
        #   Check latest SSTable index # TODO: SSTableManager class that stores all available SSTables and iterates them
        #   If key is there load it from disk using offset in SSTable index
        #   Otherwise repeat for older SSTable until found
        # Return None if key is not found
        pass
