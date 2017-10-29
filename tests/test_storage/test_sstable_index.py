import pytest
from python_kv.storage.sstable_index import SSTableIndex


# def test_get_memtable_from_binary():
#     reader = SSTableReader()
#     memtable = reader.get_memtable_from_binary(b'\x01\x00\x00\x001\x01\x00\x00\x00\x001\x01\x00\x00\x002\x01\x00\x00\x00\x002\x01\x00\x00\x003\x00\x00\x00\x00\x80')
#
#     assert memtable.get('1') == '1'
#     assert memtable.get('2') == '2'
#     assert memtable.get('3') == None


def test_get_memtable_from_binary():
    index = SSTableIndex.from_binary(b'\x93\xac\x16\x94\x00\x00\x00\x00\x17\xe2)\x01\x0c\x00\x00\x00\xb4\xa1\xc7\x0f\x17\x00\x00\x00')

    assert index.get_item_offset('1') == 0
    assert index.get_item_offset('2') == 12
    assert index.get_item_offset('3') == 23
