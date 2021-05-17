import math


# Memory buffer
class MemBuf:
    # l: low key bound, h: high key bound, n: num of cols, v: total levels of lsm tree, f: fan-out rate
    def __init__(self, capacity, level_0_cap, children):
        self.buffer = dict()
        self.capacity = capacity
        self.child_key_cap = level_0_cap
        self.children = children

    def read(self, rkeyLow, rkeyHigh, col_pos):
        if rkeyLow == rkeyHigh and rkeyLow in self.buffer:
            return self.buffer[rkeyLow]

        # The following formula figures out which child search should go to
        childLow = math.ceil(rkeyLow/self.child_key_cap) - 1
        childHigh = math.ceil(rkeyHigh/self.child_key_cap) - 1
        return self.children[childLow].read(rkeyLow, rkeyLow, col_pos)

    def write(self, wkey, wvalue):
        self.buffer[wkey] = wvalue

        if len(self.buffer) >= self.capacity:
            # make a copy of the memory buffer so it is freed for other writes
            alternative_buffer = dict(self.buffer)
            self.buffer.clear()

            # TODO: issue compaction to a different thread so its done asynchronously
            self.compaction_m2f(alternative_buffer)
            alternative_buffer.clear()

    def compaction_m2f(self, compact_buffer):
        # read in Level 0 nodes
        for node in self.children:
            node.read_whole_file()

        for key in compact_buffer:
            child = math.ceil(key / self.child_key_cap) - 1
            if key not in self.children[child].workspace:
                self.children[child].bloom_ftr.add(str(key))
            self.children[child].workspace[key] = compact_buffer[key]

            if len(self.children[child].workspace) >= self.children[child].storage_capacity:
                self.children[child].compaction_f2f()

        # compaction is done so clear memory buffer and write out every child page
        for node in self.children:
            node.write_to_file()

