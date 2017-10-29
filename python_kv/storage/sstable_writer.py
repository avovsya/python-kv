def _int_to_4_bytes(n):
    return n.to_bytes(4, signed=False, byteorder='little')

class SSTableWriter():
    def __init__(self, memtable):
        '''Initializes SSTable Writer with the memtable that needs to be written
        Args:
            memtable (MemTable): memtable to convert to SSTable
        '''
        self._memtable = memtable



    def get_binary_sstable_and_index(self):
        # SSTable binary format:
        #   4 bytes     KL bytes    4 bytes                  1 byte                  VL bytes
        #  Key length      Key     Value size      Value Flags(isTombstone, etc)       Value

        sstable_binary = bytearray()
        sstable_index = bytearray()
        current_offset_in_sstable = 0

        for key, item in self._memtable.get_sorted_items():
            key_bin = key.encode('utf-8')
            key_len = _int_to_4_bytes(len(key_bin))

            # Writing SSTable:
            sstable_binary.extend(key_len)
            sstable_binary.extend(key_bin)

            if not item.is_tombstone():
                value_bin = item.get_value().encode('utf-8')
                value_len = _int_to_4_bytes(len(value_bin))
                value_flags = int('00000000', 2).to_bytes(1, byteorder='little', signed=False)
                sstable_binary.extend(value_len)
                sstable_binary.extend(value_flags)
                sstable_binary.extend(value_bin)
            else:
                value_len = _int_to_4_bytes(0)
                value_flags = int('10000000', 2).to_bytes(1, byteorder='little', signed=False)
                sstable_binary.extend(value_len)
                sstable_binary.extend(value_flags)

            # Writing index
            key_digest = _int_to_4_bytes(item.get_key_digest())
            key_offset = _int_to_4_bytes(current_offset_in_sstable)

            sstable_index.extend(key_digest)
            sstable_index.extend(key_offset)

            current_offset_in_sstable = len(sstable_binary) + 1

        return bytes(sstable_binary), bytes(sstable_index)

