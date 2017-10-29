import os
from os import listdir
from os.path import isfile, join

from datetime import datetime

from python_kv.common.constants import FILE_NAME_DATE_FORMAT
from python_kv.storage.sstable import SSTable


class SSTableGroup():
    @classmethod
    def from_directory(cls, path):
        '''Loads multiple SSTables from a folder and initializes
        new SSTableGroupe with those tables
        Args:
            path (str): Path to a folder with tables

        Returns:
            (SSTableGroup): Instance of SSTableGroup
        '''
        def to_date(file_name):
            date = os.path.splitext(os.path.basename(file_name))[0]
            return datetime.strptime(date, FILE_NAME_DATE_FORMAT)

        group = cls()
        file_dates = [to_date(f) for f in listdir(path) if isfile(join(path, f)) and f.endswith(".table")]
        sorted_file_dates = sorted(file_dates)

        for date in sorted_file_dates:
            sstable = SSTable.from_file(path, date.strftime(FILE_NAME_DATE_FORMAT))
            group.add_table(sstable)

        return group


    def __init__(self):
        self._tables = []


    def add_table(self, table):
        '''Adds new SSTable to a group
        Args:
            table(SSTable): SSTable to add to the group
        '''
        self._tables.append(table)


    def get(self, key):
        '''Gets item from group of SSTables
        
        Args:
            key (str): Item key

        Returns:
            (str, boolean): Item value or None, boolean flag will be true if None value means Tombstone(item has been deleted)
        '''
        for table in reversed(self._tables):
            item = table.get(key)

            if item is not None:
                return item
