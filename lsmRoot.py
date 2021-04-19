from node import Node

class LsmRoot:
    # l: low key bound, h: high key bound, n: num of cols, v: total levels of lsm tree, f: fan-out rate
    def __init__(self, l, h, n, v, f):
        self.low_key_bound = l
        self.high_key_bound  = h
        self.num_of_cols = n
        self.total_levels = v
        self.fan_out = f
        self.buffer = dict()
        self.root = Node(-1, 1, 1, None)

    def read(self, key):
        if key in self.buffer:
            return self.buffer[key]
