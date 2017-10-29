import sys
import mmh3

class Item():
    def __init__(self, key, value, is_tombstone):
        self._key = str(key)
        if value is not None:
            self._value = str(value)
        else:
            self._value = None
        self._digest = mmh3.hash(self._key, signed=False)
        self._is_tombstone = is_tombstone

    def get_key_digest(self):
        return self._digest

    # @staticmethod
    # def get_hash(key):
    #     return mmh3.hash(key)

    def get_key(self):
        return self._key

    def get_value(self):
        return self._value

    def is_tombstone(self):
        return self._is_tombstone

    def get_size(self):
        return sys.getsizeof(self._key) + sys.getsizeof(self._value)
