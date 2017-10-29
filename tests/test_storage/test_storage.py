import pytest

from datetime import datetime, timedelta

from python_kv.common.constants import FILE_NAME_DATE_FORMAT
from python_kv.storage.item import Item
from python_kv.storage.memtable import Memtable
from python_kv.storage.sstable import SSTable
from python_kv.storage.storage import Storage


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

    storage = Storage(test_folder)

    storage.put(4, "in memtable")

    assert "1" == storage.get(1)
    assert "2" == storage.get(2)
    assert None == storage.get(3)
    assert "in memtable" == storage.get(4)
