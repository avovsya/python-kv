import pytest

from datetime import datetime, timedelta

from python_kv.common.constants import FILE_NAME_DATE_FORMAT
from python_kv.storage.item import Item
from python_kv.storage.memtable import Memtable
from python_kv.storage.sstable import SSTable
from python_kv.storage.sstable_group import SSTableGroup


def test_loading_from_folder(test_folder):
    now = datetime.now()
    hour_after = datetime.now() + timedelta(hours=1)
    hour_before = datetime.now() - timedelta(hours=1)
    day_before = datetime.now() - timedelta(days=1)

    mm1 = Memtable()
    mm1.put(1, 1)
    mm1.delete(3)

    mm2 = Memtable()
    mm2.put(2, 2)

    mm3 = Memtable()
    mm3.put(1, "old value")
    mm3.put(3, "should be deleted")
    mm3.put(4, 4)

    SSTable.from_memtable(mm1, test_folder, now.strftime(FILE_NAME_DATE_FORMAT))
    SSTable.from_memtable(mm2, test_folder, hour_before.strftime(FILE_NAME_DATE_FORMAT))
    SSTable.from_memtable(mm3, test_folder, day_before.strftime(FILE_NAME_DATE_FORMAT))

    sstable_group = SSTableGroup.from_directory(test_folder)

    assert Item(1, 1, False) == sstable_group.get(1)
    assert Item(2, 2, False) == sstable_group.get(2)
    assert Item(3, None, True) == sstable_group.get(3)
    assert Item(4, 4, False) == sstable_group.get(4)

    mm4 = Memtable()
    mm4.put(5, 5)
    mm4.delete(1)

    new_sstable = SSTable.from_memtable(mm4, test_folder, hour_after.strftime(FILE_NAME_DATE_FORMAT))
    sstable_group.add_table(new_sstable)

    assert Item(1, None, True) == sstable_group.get(1)
    assert Item(5, 5, False) == sstable_group.get(5)
