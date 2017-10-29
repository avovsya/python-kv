import pytest
from python_kv.storage.memtable import MemTable
from python_kv.storage.sstable_writer import SSTableWriter

def test_binary_output():
    memtable =  MemTable()
    memtable.put(1, 1)
    memtable.put(2, 2)
    memtable.put(3, 3)
    memtable.delete(3)

    writer = SSTableWriter(memtable)
    sstable, index = writer.get_binary_sstable_and_index()
    assert sstable == b'\x01\x00\x00\x001\x01\x00\x00\x00\x001\x01\x00\x00\x002\x01\x00\x00\x00\x002\x01\x00\x00\x003\x00\x00\x00\x00\x80'
    assert index == b'\x93\xac\x16\x94\x00\x00\x00\x00\x17\xe2)\x01\x0c\x00\x00\x00\xb4\xa1\xc7\x0f\x17\x00\x00\x00'

