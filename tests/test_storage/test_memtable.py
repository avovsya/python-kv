import pytest
import sys

from python_kv.storage.item import Item
from python_kv.storage.memtable import Memtable


def test_retrieval():
    memtable = Memtable()
    memtable.put("key1", "value1")
    memtable.put("key2", "value2")
    memtable.put("key2", "value2-changed")

    assert memtable.get('key1') == Item('key1', "value1", False)
    assert memtable.get('key2') == Item('key2', "value2-changed", False)


def test_deletion():
    memtable = Memtable()
    memtable.put("key1", "value1")
    memtable.put("key1", "value1-changed")
    memtable.delete("key1")

    assert memtable.get('key1').is_tombstone() == True


def test_size():
    key1_size = sys.getsizeof("key1") + sys.getsizeof("value1")
    key1_changed_size = sys.getsizeof("key1") + sys.getsizeof("value1-changed")
    key2_size = sys.getsizeof("key2") + sys.getsizeof("value2")
    key3_size = sys.getsizeof("key3") + sys.getsizeof("value3")
    key3_deleted_size = sys.getsizeof("key3") + sys.getsizeof(None)

    memtable = Memtable()

    memtable.put("key1", "value1")
    memtable.put("key2", "value2")
    assert memtable.get_size() == key1_size + key2_size

    memtable.put("key1", "value1-changed")
    memtable.put("key3", "value3")
    assert memtable.get_size() == key1_changed_size + key2_size + key3_size

    memtable.delete("key3")
    assert memtable.get_size() == key1_changed_size + key2_size + key3_deleted_size


def test_sorted_items_iterator():
    memtable = Memtable()

    memtable.put("key2", "value2")
    memtable.put("key1", "value1")
    memtable.put("4", "4")
    memtable.put("key3", "value3")
    memtable.put("33", "33")
    memtable.put("5", "5")

    output = ''

    for key, _ in memtable.get_sorted_items():
        output += key + ":"

    assert output == "33:4:5:key1:key2:key3:"


def test_binary_output():
    memtable =  Memtable()
    memtable.put(1, 1)
    memtable.put(2, 2)
    memtable.put(3, 3)
    memtable.delete(3)

    binary_sstable, binary_index, index = memtable.convert_to_binary_and_index()
    assert binary_sstable == b'\x01\x00\x00\x001\x01\x00\x00\x00\x001\x01\x00\x00\x002\x01\x00\x00\x00\x002\x01\x00\x00\x003\x00\x00\x00\x00\x80'
    assert binary_index == b'\x93\xac\x16\x94\x00\x00\x00\x00\x17\xe2)\x01\x0b\x00\x00\x00\xb4\xa1\xc7\x0f\x16\x00\x00\x00'
    assert index.get_item_offset('1') == 0
    assert index.get_item_offset('2') == 11
    assert index.get_item_offset('3') == 22

