from python_kv.storage.item import Item


class SSTableIndex():
    def __init__(self):
        self._dict = dict()


    def add_item(self, key, offset):
        item = Item(key, None, False)
        self._dict[item.get_key_digest()] = offset


    def add_item_by_key_digest(self, key_digest, offset):
        self._dict[key_digest] = offset


    def get_item_offset(self, key):
        item = Item(key, None, False)
        return self._dict.get(item.get_key_digest(), None)
