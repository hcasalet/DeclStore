import math
from node import Node


class MemBuf:
    # l: low key bound, h: high key bound, n: num of cols, v: total levels of lsm tree, f: fan-out rate
    def __init__(self, low_bound, high_bound, cols, levels, fan_out):
        self.low_key_bound = low_bound
        self.high_key_bound = high_bound
        self.num_of_cols = cols
        self.total_levels = levels
        self.fan_out = fan_out
        self.buffer = dict()
        self.root = Node(-1, 1, 1, None)
        self.children = []

    def read(self, key):
        if key in self.buffer:
            return self.buffer[key]

        # The following formula figures out which child search should go to
        child = math.ceil(key / math.ceil((self.high_key_bound - self.low_key_bound + 1) / self.fan_out)) - 1
        if not self.children[child]:
            return None
        else:
            return self.children[child].read_from_child(self.children[child].get_file_name)
