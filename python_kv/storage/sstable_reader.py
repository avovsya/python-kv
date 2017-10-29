from python_kv.storage.memtable import MemTable
from python_kv.storage.sstable_index import SSTableIndex
from python_kv.storage.item import Item

def _get_value_flags_from_bit_flags(bit_flags):
    is_tombstone_bit_mask = int('10000000', 2)
    return {
        "is_tombstone": is_tombstone_bit_mask & bit_flags
    }


def _get_memtable_item_from_bytes(bytes, current_offset):
    key_len = int.from_bytes(bytes[current_offset:current_offset+4], byteorder='little', signed=False)
    current_offset += 4

    key_binary = bytes[current_offset:current_offset+key_len]
    key = key_binary.decode('utf-8')

    current_offset += key_len

    value_len = int.from_bytes(bytes[current_offset:current_offset+4], byteorder='little', signed=False)
    bit_flags = int.from_bytes(bytes[current_offset+4:current_offset+5], byteorder='little', signed=False)

    current_offset += 5

    value_binary = bytes[current_offset:current_offset+value_len]
    value = value_binary.decode('utf-8')

    flags = _get_value_flags_from_bit_flags(bit_flags)

    end_offset = current_offset + value_len

    return Item(key, value, flags["is_tombstone"]), end_offset


class SSTableReader():
    def get_memtable_from_binary(self, bytes):
        memtable = MemTable()
        current_offset = 0

        item, end_offset = _get_memtable_item_from_bytes(bytes, current_offset)

        memtable.put_item(item)

        while end_offset < len(bytes):
            item, end_offset = _get_memtable_item_from_bytes(bytes, end_offset)
            memtable.put_item(item)

        return memtable


    def get_index_from_binary(self, bytes):
        index = SSTableIndex()

        offset = 0

        while offset < len(bytes):
            key_digest = int.from_bytes(bytes[offset:offset+4], byteorder='little', signed=False)
            key_offset = int.from_bytes(bytes[offset+4:offset+8], byteorder='little', signed=False)

            index.add_item_by_key_digest(key_digest, key_offset)

            offset += 8

        return index

