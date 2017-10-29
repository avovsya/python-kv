from sortedcontainers import SortedDict

from python_kv.storage.item import Item


class MemTable():
    def __init__(self):
        self._sorted_dict = SortedDict()
        self._size = 0


    def _get_item(self, key):
        item = self._sorted_dict.get(key, None)
        if item is None:
            return None

        if item.is_tombstone():
            return None

        return item


    def _dec_size_if_key_exists(self, key):
        item = self._get_item(key)
        if item is not None:
            self._size -= item.get_size()


    def _inc_size(self, item):
        if item is not None:
            self._size += item.get_size()


    def get(self, key):
        item = self._get_item(key)
        if item is not None:
            return item.get_value()

        return None

    def put_item(self, item):
        self._dec_size_if_key_exists(item.get_key())
        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def put(self, key, value):
        item = Item(key, value, False)
        self._dec_size_if_key_exists(item.get_key())

        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def delete(self, key):
        item = Item(key, None, True)
        self._dec_size_if_key_exists(item.get_key())

        self._inc_size(item)
        self._sorted_dict[item.get_key()] = item


    def get_size(self):
        return self._size


    def get_sorted_items(self):
        return self._sorted_dict.items()
