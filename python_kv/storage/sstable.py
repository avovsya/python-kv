import os

from python_kv.storage.item import Item
from python_kv.storage.sstable_index import SSTableIndex


def _get_value_flags_from_bit_flags(bit_flags):
    is_tombstone_bit_mask = int('10000000', 2)
    return {
        "is_tombstone": bool(is_tombstone_bit_mask & bit_flags)
    }


def _get_memtable_item_from_file(file, offset):
    file.seek(offset)
    key_len = int.from_bytes(file.read(4), byteorder='little', signed=False)

    key_binary = file.read(key_len)
    key = key_binary.decode('utf-8')

    value_len = int.from_bytes(file.read(4), byteorder='little', signed=False)
    bit_flags = int.from_bytes(file.read(1), byteorder='little', signed=False)

    if value_len != 0:
        value_binary = file.read(value_len)
        value = value_binary.decode('utf-8')
    else:
        value = None

    flags = _get_value_flags_from_bit_flags(bit_flags)

    return Item(key, value, flags["is_tombstone"])


class SSTable:
    @classmethod
    def from_file(cls, path, file_prefix):
        '''Loads SSTable from file and returns instance of SSTable class
        Args:
            path (str): Path to a folder with tables and table indexes
            file_prefix (str): File prefix for this SSTable, e.g.: "2017-10-29T14:09:09"

        Returns:
            (SSTable): SSTable initialized from a table on disk
        '''
        table_path = os.path.join(path, "{}.table".format(file_prefix))
        index_path = os.path.join(path, "{}.index".format(file_prefix))

        with open(index_path, 'rb') as index_file:
            index_binary = index_file.read()

        memory_index = SSTableIndex.from_binary(index_binary)

        return cls(memory_index, table_path)


    @classmethod
    def from_memtable(cls, memtable, path, file_prefix):
        '''Creates SSTable from Memtable 
        Args:
            memtable (Memtable): Source of items for SSTable being created
            path (str): Path to a folder with tables and table indexes
            file_prefix (str): File prefix for this SSTable, e.g.: "2017-10-29T14:09:09"

        Returns:
            (SSTable): SSTable initialized from a table on disk
        '''
        table_path = os.path.join(path, "{}.table".format(file_prefix))
        index_path = os.path.join(path, "{}.index".format(file_prefix))

        binary_table, binary_index, memory_index = memtable.convert_to_binary_and_index()

        with open(table_path, 'wb') as table_file:
            table_file.write(binary_table)

        with open(index_path, 'wb') as index_file:
            index_file.write(binary_index)

        return cls(memory_index, table_path)


    def __init__(self, index, table_filename):
        '''Initialize SSTable class with in-memory index and path to a binary file containing table
        
        Args:
            index (SSTableIndex): In-memory SSTable index
            table_filename (str): Path to a file containing table in binary format
        '''
        self.index = index
        self.table_filename = table_filename


    def get(self, key):
        '''Gets item from SSTable 
        
        Args:
            key (str): Item key

        Returns:
            (str, boolean): Item value or None, boolean flag will be true if None value means Tombstone(item has been deleted)
        '''
        item_offset = self.index.get_item_offset(key)

        if item_offset is None:
            return (None, False)

        with open(self.table_filename, 'rb') as table_file:
            item = _get_memtable_item_from_file(table_file, item_offset)

            return item.get_value(), item.is_tombstone()

