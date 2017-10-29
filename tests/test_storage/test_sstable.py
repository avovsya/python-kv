import os

import pytest

from python_kv.storage.memtable import Memtable
from python_kv.storage.sstable import SSTable


def test_creation_from_memtable(test_folder):
    memtable = Memtable()
    memtable.put(1, 1)

    sstable = SSTable.from_memtable(memtable, test_folder, 'test')

    expected_table_path = os.path.join(test_folder, "test.table")
    expected_index_path = os.path.join(test_folder, "test.index")

    assert os.path.isfile(expected_table_path)
    assert os.path.isfile(expected_index_path)

    with open(expected_table_path, 'rb') as f:
        table = f.read()
        assert table == b'\x01\x00\x00\x001\x01\x00\x00\x00\x001'

    with open(expected_index_path, 'rb') as f:
        index = f.read()
        assert index == b'\x93\xac\x16\x94\x00\x00\x00\x00'


def test_get_value_memtable_created(test_folder):
    memtable = Memtable()
    memtable.put('1', '1')
    memtable.put('2', '2')
    memtable.put('3', '3')
    memtable.put('2', 'two')
    memtable.delete('3')

    sstable = SSTable.from_memtable(memtable, test_folder, 'test')

    assert ('1', False) == sstable.get('1')
    assert ('two', False) == sstable.get('2')
    assert (None, True) == sstable.get('3')
    assert (None, False) == sstable.get('4')

