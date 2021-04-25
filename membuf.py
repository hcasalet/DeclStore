import math
from node import Node


# Memory buffer which resides with client
class MemBuf:
    # l: low key bound, h: high key bound, n: num of cols, v: total levels of lsm tree, f: fan-out rate
    def __init__(self, key_range, cols, total_levels, fan_out, node_storage_capacity, file_root, fp_prob):
        self.buffer = dict()
        self.capacity = math.ceil(key_range / pow(fan_out, total_levels))
        self.child_key_cap = math.ceil(key_range / fan_out)
        self.children = self.make_children(fan_out, self.child_key_cap, cols, total_levels, node_storage_capacity, file_root, fp_prob)

    @classmethod
    def make_children(cls, num_children, child_key_cap, cols, total_levels, node_storage_capacity, fr, fp_prob):
        children = []
        child_range_low_bound = 1
        child_range_high_bound = child_key_cap

        for child in range(num_children):
            node = Node(child_range_low_bound, child_range_high_bound, cols, total_levels,
                        node_storage_capacity, 0, child+1, 1, num_children, fr, fp_prob)
            children.append(node)
            child_range_low_bound = child_range_high_bound + 1
            child_range_high_bound = child_range_low_bound + child_key_cap - 1

        return children

    def read(self, rkey, col_pos):
        if rkey in self.buffer:
            return self.buffer[rkey]

        # The following formula figures out which child search should go to
        child = math.ceil(rkey/self.child_key_cap) - 1
        if not self.children[child]:
            return None
        else:
            return self.children[child].read(rkey, col_pos)

    def write(self, wkey, wvalue):
        self.buffer[wkey] = wvalue

        if len(self.buffer) >= self.capacity:
            # TODO: issue compaction to a different thread so its done asynchronously
            self.compaction_m2f()

    def compaction_m2f(self):
        # read in Level 0 nodes
        for node in self.children:
            node.read_whole_file()

        for key in self.buffer:
            child = math.ceil(key / self.child_key_cap) - 1
            if key not in self.children[child].workspace:
                self.children[child].bloom_ftr.add(str(key))
            self.children[child].workspace[key] = self.buffer[key]

            if len(self.children[child].workspace) >= self.children[child].storage_capacity:
                self.children[child].compaction_f2f()

        # compaction is done so clear memory buffer and write out every child page
        self.buffer.clear()
        for node in self.children:
            node.write_to_file()
