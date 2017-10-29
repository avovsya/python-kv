import pytest
import sys
from python_kv.storage.memtable import MemTable


def test_retrieval():
    memtable = MemTable()
    memtable.put("key1", "value1")
    memtable.put("key2", "value2")
    memtable.put("key2", "value2-changed")

    assert memtable.get('key1') == "value1"
    assert memtable.get('key2') == "value2-changed"


def test_deletion():
    memtable = MemTable()
    memtable.put("key1", "value1")
    memtable.put("key1", "value1-changed")
    memtable.delete("key1")

    assert memtable.get('key1') is None


def test_size():
    key1_size = sys.getsizeof("key1") + sys.getsizeof("value1")
    key1_changed_size = sys.getsizeof("key1") + sys.getsizeof("value1-changed")
    key2_size = sys.getsizeof("key2") + sys.getsizeof("value2")
    key3_size = sys.getsizeof("key3") + sys.getsizeof("value3")
    key3_deleted_size = sys.getsizeof("key3") + sys.getsizeof(None)

    memtable = MemTable()

    memtable.put("key1", "value1")
    memtable.put("key2", "value2")
    assert memtable.get_size() == key1_size + key2_size

    memtable.put("key1", "value1-changed")
    memtable.put("key3", "value3")
    assert memtable.get_size() == key1_changed_size + key2_size + key3_size

    memtable.delete("key3")
    assert memtable.get_size() == key1_changed_size + key2_size + key3_deleted_size


def test_sorted_items_iterator():
    memtable = MemTable()

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
