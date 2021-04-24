import json
import math
from bloomfilter import BloomFilter


class Node(object):
    def __init__(self, key_low_bound, key_high_bound,
                 columns, level, group, position,
                 fan_out, file_root, fp_prob):
        # root directory of where the file for this lsmtree node
        self.file_root = file_root

        # the low bound (inclusive) of key range
        self.key_low_bound = key_low_bound

        # the high bound (inclusive) of key range
        self.key_high_bound = key_high_bound

        # total number of columns
        self.num_columns = columns

        # the horizontal level of the lsmtree the node is on
        self.level = level

        # the column group in the level this node is in
        self.group = group

        # the position inside a column group this node is in
        self.col_position = position

        # number of children
        self.fan_out = fan_out

        # list of children
        self.children = []

        # the bloom filter for the keys stored in this node
        items = key_high_bound - key_low_bound + 1
        self.bloom = BloomFilter(items, fp_prob)

    def read(self, read_key):
        filename = self.get_file_name()

        with open(filename, "rb") as infile:
            # Read the first 512 bytes which have the bloom filter bytes
            data = infile.read(512)

            # parse the "header" (fist 512 bytes) to figure out the bloom filter bytes
            self.get_bloom_filter(data)

    def get_child_file_name(self, read_key, col_pos):
        # actual fan-out has to take number of column groups into consideration
        child_level = self.level + 1

        # each node will have fan-out/2 groups in their kids
        child_group_base = (self.group - 1) * self.fan_out / 2

        # for any children, their key range is the parent's key range split into fan-out number
        # of ways multiplied by 2. The "multiplying by 2" part is to compensate the fact that each
        # level down the column group splits into 2 so the range has to only split by fan-out/2, not
        # fan-out. The exception is from memory to level 0, where the column group in level 0 is 1,
        # so in this case the key range will be split by fan-out.
        my_key_range = (self.key_high_bound - self.key_low_bound + 1)
        if self.level == -1:
            child_key_range = math.ceil(my_key_range / self.fan_out)
        else:
            child_key_range = math.ceil(my_key_range / self.fan_out * 2)
        child_group_addition = (read_key - self.key_low_bound) / child_key_range + 1
        child_group = child_group_base + child_group_addition

        col_group_size = self.num_columns / math.pow(2, self.level)
        child_col_position = ((col_pos - 1) / col_group_size) % 2 + 1

        return self.get_file_name(child_level, child_group, child_col_position)

    def get_bloom_filter(self, data):
        # the first 4 bytes is the length of the bloom filter
        length = data[:4]
        len = int()
        print(length)

    @classmethod
    def get_file_name(cls, lvl, grp, pos):
        return cls.file_root + '/level-' + str(lvl) + '/group-' + str(grp) + '/position-' + str(pos) + '.log'
