import math
import os
from pathlib import Path
import pickle
from bloomfilter import BloomFilter

'''
 $$$$ File Naming Convention: 
 ** The entire path starting from level 0 is specified in the name **
 - Between levels a "/" is used to connect them, so a parent level is corresponding to a parent directory in file 
   system.
 - Within each level it takes the format of "lv-<i>.kr-<j>.cg-<k>", where i indicates it's the level of the LSM tree
   the node is on, j is the order of the key range piece within its parent node's key range, and k is the order of the 
   column group within the key range. 
   
   Example: lv-0.kr-1.cg-1/lv-1.kr-3.cg-1/lv-2.kr-2.cg-2/data.log,
            lv-0-1-1/lv-1.kr-3.cg-1/data.log
            lv-0-1-1/data.log
        
        C:.
        |----lv-0.kr-1.cg-1
        |       |__data.log
        |       |__lv-1.kr-1.cg-1
        |       |       |__data.log
        |       |       |__lv-2.kr-1.cg-1
        |       |       |       |__data.log
        |       |       |__lv-2.kr-1.cg-2
        |       |       |       |__data.log
        |       |__lv-1.kr-1.cg-2
        |       |__lv-1.kr-2.cg-1
        |----lv-0.kr-2.cg-1
            
 $$$$ File content:
  - bloom filter. Bloom filter will only be added when a key is written into a file, and will never be cleared even 
    when the data get compacted into the nodes in the levels below. When a key is deleted, the bits indicating 
    the existence of the key also won't be reset back to zero. Therefore, as more keys have been deleted, the bloom 
    filter false positive rate will likely increase. When the FP rate is higher than certain rate, we will trigger 
    a rebuild of bloom filter.
  - Children nodes column groups meta data. This records for every child key range (the concatenation of all children's
    distinct key ranges will make up the parent's key range) their column group information, ie, which columns a 
    particular child holds data for.
  - the actual data
                   
'''


class Node:
    def __init__(self, key_low_bound, key_high_bound, total_levels, storage_capacity,
                 level, key_range_order, column_group, fan_out, file_root, fp_prob):
        # root directory of where the file for this lsmtree node
        self.file_root = file_root

        # the low bound (inclusive) of key range
        self.key_low_bound = key_low_bound

        # the high bound (inclusive) of key range
        self.key_high_bound = key_high_bound

        # total number of levels (this is to know if you are the last level)
        self.total_levels = total_levels

        # storage capacity for data
        self.storage_capacity = storage_capacity

        # the horizontal level of the lsmtree the node is on
        self.level = level

        # the child order among children of its parent
        self.key_range_order = key_range_order

        # the position inside a column group this node is in
        self.column_group = column_group

        # number of children
        self.fan_out = fan_out

        # list of children
        if self.level < self.total_levels - 1:
            self.children = self.make_node_children(fp_prob)
        else:
            self.children = []

        # the bloom filter for the keys stored in this node
        self.bloom_ftr = BloomFilter((self.key_high_bound - self.key_low_bound + 1), fp_prob)

        # children column group map
        self.children_column_group_map = dict()

        # the workspace in memory when data is read in from the file during compaction
        self.workspace = dict()

    def make_node_children(self, fp_prob):
        child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out)
        child_range_start = self.key_low_bound

        col_position_in_group = 0
        children = []
        for ch in range(self.fan_out):
            child_range_end = child_range_start + child_key_cap - 1
            filepath = self.file_root + '/lv-' + str(self.level+1) + '.kr-' + str(ch+1) + '.cg-1'
            Path(filepath).mkdir(parents=True, exist_ok=True)
            chnode = Node(child_range_start, child_range_end, self.total_levels,
                          self.storage_capacity, self.level+1, ch+1, 1, self.fan_out, filepath, fp_prob)
            children.append(chnode)
            child_range_start = child_range_end + 1

        return children

    def init_column_group(self):
        if self.level == self.total_levels - 2:
            for child in range(self.fan_out):
                self.children_column_group_map[child+1] = [(self.key_low_bound, self.key_high_bound)]

    # Read the value of a key
    def read(self, rkeyLow, rkeyHigh, col_pos):
        # What is the filename for myself
        filename = self.get_file_name()
        if not os.path.exists(filename):
            return None

        # reconstruct the bloom filter from the file
        bf, bf_length = BloomFilter.read_bloom_filter_from_file(filename)

        # bloom filter shows the item is possibly in, so lets getting it
        if not bf.check(str(rkeyLow)):
            return None
        else:
            obj = self.read_data(filename, bf_length, rkeyLow)
            if obj is not None:
                return obj
            else:  # bloom filter was false positive, continue to search children
                if self.level >= self.total_levels - 1:
                    return None
                else:
                    child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out)
                    child = math.ceil((rkeyLow - self.key_low_bound + 1) / child_key_cap) - 1
                    if not self.children:
                        return None
                    else:
                        return self.children[child].read(rkeyLow, rkeyHigh, col_pos)

    # Read the bloom filter and the key/value pairs into memory for compaction
    def read_whole_file(self):
        filename = self.get_file_name()
        if os.path.exists(filename):
            with open(filename, "rb") as infile:
                alldata = infile.read()
            length = int.from_bytes(alldata[:4], 'big')
            self.bloom_ftr = pickle.loads(alldata[4:(4+length)])
            cg_map_len = int.from_bytes(alldata[(4+length):(8+length)], 'big')
            self.children_column_group_map = pickle.loads(alldata[(8+length):(8+length+cg_map_len)])
            self.workspace = pickle.loads(alldata[(8+length+cg_map_len):])

    # write the content of the node to a file
    def write_to_file(self):
        bf_bytes = self.bloom_ftr.prepare_bloom_filter_to_write()

        cgmap_bytes = pickle.dumps(self.children_column_group_map)
        cgmap_length = len(cgmap_bytes)
        cgmap_len_bytes = cgmap_length.to_bytes(4, 'big')
        cgmap_with_len = cgmap_len_bytes + cgmap_bytes

        obj_data_bytes = pickle.dumps(self.workspace)
        #data_to_write = bf_bytes + obj_data_bytes
        data_to_write = bf_bytes + cgmap_with_len + obj_data_bytes

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

        child_key_cap = math.ceil((self.key_high_bound - self.key_low_bound + 1) / self.fan_out)
        for key in self.workspace:
            kiddo = math.ceil((key - self.key_low_bound + 1) / child_key_cap) - 1
            if key not in self.children[kiddo].workspace:
                self.children[kiddo].bloom_ftr.add(str(key))
            self.children[kiddo].workspace[key] = self.workspace[key]

            if len(self.children[kiddo].workspace) >= self.children[kiddo].storage_capacity:
                self.children[kiddo].compaction_f2f()

        # compact is done so clear myself
        self.workspace.clear()
        for child in self.children:
            child.write_to_file()

    @classmethod
    def read_data(cls, filename, start_length, read_key):
        with open(filename, "rb") as infile:
            indata = infile.read()

        cgmap_len = int.from_bytes(indata[start_length:(start_length+4)], 'big')
        cgmap = pickle.loads(indata[(start_length+4):(start_length+4+cgmap_len)])
        obj_data = pickle.loads(indata[(start_length+4+cgmap_len):])

        if obj_data and obj_data[read_key]:
            return obj_data[read_key]
        else:
            return None

    def get_file_name(self):
        return self.file_root + '/data.log'


if __name__ == '__main__':
    n = Node(1, 10, 100, 0, 1, 1, 8, '/Users/hollycasaletto', 0.05)
    n.read('twitter', 100)
    n.read('happy', 100)