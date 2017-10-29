from python_kv.storage.item import Item


class SSTableIndex():
    @classmethod
    def from_binary(cls, binary_index):
        '''  Loads Index from binary index representation
        Args:
            binary_index (bytes): Binary index

        Returns:
            (SSTableIndex): SSTableIndex instance representing data from binary index
        '''

        index = SSTableIndex()

        offset = 0

        while offset < len(binary_index):
            key_digest = int.from_bytes(binary_index[offset:offset+4], byteorder='little', signed=False)
            key_offset = int.from_bytes(binary_index[offset+4:offset+8], byteorder='little', signed=False)

            index.add_item_by_key_digest(key_digest, key_offset)

            offset += 8

        return index


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
