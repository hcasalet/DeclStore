import math
import os
import pickle
from bloomfilter import BloomFilter


class Node:
    def __init__(self, key_low_bound, key_high_bound, columns,
                 total_levels, storage_capacity, level, group,
                 position, fan_out, file_root, fp_prob):
        # root directory of where the file for this lsmtree node
        self.file_root = file_root

        # the low bound (inclusive) of key range
        self.key_low_bound = key_low_bound

        # the high bound (inclusive) of key range
        self.key_high_bound = key_high_bound

        # total number of columns
        self.num_columns = columns

        # total number of levels (this is to know if you are the last level)
        self.total_levels = total_levels

        # storage capacity for data
        self.storage_capacity = storage_capacity

        # the horizontal level of the lsmtree the node is on
        self.level = level

        # the column group in the level this node is in
        self.group = group

        # the position inside a column group this node is in
        self.col_position = position

        # number of children
        self.fan_out = fan_out

        # list of children
        if self.level < self.total_levels - 1:
            self.children = self.make_node_children(fp_prob)
        else:
            self.children = []

        # the bloom filter for the keys stored in this node
        self.bloom_ftr = BloomFilter((self.key_high_bound - self.key_low_bound + 1), fp_prob)

        # the workspace in memory when data is read in from the file during compaction
        self.workspace = dict()

    def make_node_children(self, fp_prob):
        child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out * 2)
        child_range_start = self.key_low_bound

        col_position_in_group = 0
        children = []
        for ch in range(self.fan_out):
            child_range_end = child_range_start + child_key_cap - 1
            chnode = Node(child_range_start, child_range_end, self.num_columns, self.total_levels,
                          self.storage_capacity, self.level+1, math.ceil((ch+1) / 2),
                          (col_position_in_group % 2) + 1, self.fan_out, self.file_root, fp_prob)
            children.append(chnode)
            child_range_start = child_range_end + 1

        return children

    # Read the value of a key
    def read(self, read_key, col_pos):
        # What is the filename for myself
        filename = self.get_file_name()
        if not os.path.exists(filename):
            return None

        # reconstruct the bloom filter from the file
        bf, bf_length = BloomFilter.read_bloom_filter_from_file(filename)

        # bloom filter shows the item is possibly in, so lets getting it
        if bf.check(str(read_key)):
            obj = self.read_data(filename, bf_length, read_key)
            if obj is not None:
                return obj

        # bloom filter was false positive, continue to search children
        if self.level >= self.total_levels - 1:
            return None
        else:
            child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out * 2)
            child = math.ceil((read_key - self.key_low_bound + 1) / child_key_cap) - 1
            if not self.children:
                return None
            else:
                self.children[child].read(read_key, col_pos)

    # Read the bloom filter and the key/value pairs into memory for compaction
    def read_whole_file(self):
        filename = self.get_file_name()
        if os.path.exists(filename):
            with open(filename, "rb") as infile:
                alldata = infile.read()
            length = int.from_bytes(alldata[:4], 'big')
            self.bloom_ftr = pickle.loads(alldata[4:(4+length)])
            self.workspace = pickle.loads(alldata[(4+length):])

    # write the content of the node to a file
    def write_to_file(self):
        bf_bytes = self.bloom_ftr.prepare_bloom_filter_to_write()
        obj_data_bytes = pickle.dumps(self.workspace)
        data_to_write = bf_bytes + obj_data_bytes

        with open(self.get_file_name(), "wb") as outfile:
            outfile.write(data_to_write)
        outfile.close()

        # TODO: check if clearing is not needed. If not then do not for performance reason
        self.bloom_ftr.clear()
        self.workspace.clear()

    def compaction_f2f(self):
        if self.level >= self.total_levels - 1:
            return

        for child in self.children:
            child.read_whole_file()

        child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out * 2)
        for key in self.workspace:
            kiddo = math.ceil((key - self.key_low_bound + 1) / child_key_cap) - 1
            self.children

    @classmethod
    def read_data(cls, filename, bf_length, read_key):
        with open(filename, "rb") as infile:
            indata = infile.read()
        obj_data = pickle.loads(indata[bf_length:])
        try:
            return obj_data[read_key]
        except ValueError:
            print('Reading data ran into exception!')

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

    def get_file_name(self):
        return self.file_root + '/level-' + str(self.level) + '_group-' + str(self.group) + \
               '_position-' + str(self.col_position) + '.log'


if __name__ == '__main__':
    n = Node(1, 10, 100, 0, 1, 1, 8, '/Users/hollycasaletto', 0.05)
    n.read('twitter', 100)
    n.read('holly', 100)