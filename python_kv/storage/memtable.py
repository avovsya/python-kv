from sortedcontainers import SortedDict

from python_kv.storage.item import Item
from python_kv.storage.sstable_index import SSTableIndex


def _int_to_4_bytes(n):
    return n.to_bytes(4, signed=False, byteorder='little')


class Memtable():
    def __init__(self):
        self._sorted_dict = SortedDict()
        self._size = 0


    # def _get_item(self, key):
    #     item = self._sorted_dict.get(key, None)
    #     if item is None:
    #         return None
    #
    #     if item.is_tombstone():
    #         return None
    #
    #     return item


    def _dec_size_if_key_exists(self, key):
        # item = self._get_item(key)
        item = self.get(key)
        if item is not None:
            self._size -= item.get_size()


    def _inc_size(self, item):
        if item is not None:
            self._size += item.get_size()


    def get(self, key):
        # item = self._get_item(key)
        # if item is not None:
        #     return item.get_value()
        item = Item(key, None, False)

        return self._sorted_dict.get(item.get_key(), None)

    def put_item(self, item):
        self._dec_size_if_key_exists(item.get_key())
        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def put(self, key, value):
        item = Item(key, value, False)
        self._dec_size_if_key_exists(item.get_key())

        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def delete(self, key):
        item = Item(key, None, True)
        self._dec_size_if_key_exists(item.get_key())

        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def get_size(self):
        return self._size


    def get_sorted_items(self):
        return self._sorted_dict.items()


    def convert_to_binary_and_index(self):
        '''Converts Memtable to binary sstable, binary index and in-memory index

        Returns:
            (bytes, bytes, SSTableIndex): binary sstable, binary index, in-memory index

        '''
        # SSTable binary format:
        #   4 bytes     KL bytes    4 bytes                  1 byte                  VL bytes
        #  Key length      Key     Value size      Value Flags(isTombstone, etc)       Value

        sstable_binary = bytearray()
        sstable_index = bytearray()
        index = SSTableIndex()
        current_offset_in_sstable = 0

        for key, item in self.get_sorted_items():
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

            # Writing in-memory index
            index.add_item(item.get_key(), current_offset_in_sstable)

            sstable_index.extend(key_digest)
            sstable_index.extend(key_offset)

            current_offset_in_sstable = len(sstable_binary)

        return bytes(sstable_binary), bytes(sstable_index), index
