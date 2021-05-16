import math
from membuf import MemBuf
from node import Node
from pathlib import Path

'''
                                 ----- Design of the LSM Tree -----
                                 
                                          _______________
                                          | MemoryBuffer|
                                          ---------------
                                                | X10
                 ----------------------------------------------------------------
                 |              |               |             ....              |
  Level 0:      ◉◉◉            ◉◉◉             ◉◉◉                             ◉◉◉     (1% of data)
                                | X10
                 --------------------------------------------------------    
                 |              |               |        .....          |
  Level 1:      ◉◉◉            ◉◉◉             ◉◉◉                     ◉◉◉             (10% of data, row-based)
                                                | XAdaptive
                          ----------------------------------------------------------
                          |   |          |            | | |            |      ......
  Level 2:                ◉  ◉◉         ◉◉◉           ◉ ◉ ◉           ◉◉◉              (100% capacity)
  
  
  1. Tree root is the memory buffer, and three levels. Level 0 and level 1 are row based, and level 2 is column based.
  2. Key range [keyLow, keyHigh], with a fan-out rate of 10 from the memory buffer to level 0, and each level i to 
     level i+1. With this structure, the maximum data capacity of level 0 is 1% of the data, and level 1 10%, level 2
     100%. Every child node holds 10% of its parent node's key range. There are no restrictions to the size of the 
     memory buffer.
  3. Level 0 and level 1 hold all columns in one entire row together, while level 2 adaptively have pieces (subset) of 
     columns depending on the incoming query workload. (column cracking)
  4. A level 1 node has the information about the column pieces among its children in its 1st page. 
  5. The compaction from every level including the memory buffer to its children level is triggered when the node is
     full, while a receiving child node could in turn become full during the compaction and start a downward compaction 
     to the child node's children.

'''


class LsmTree:
    def __init__(self, items, levels, fan_out, file_root, fp_prob):
        # number of levels is log[num_cols], starting at level 0 and thus +1
        self.levels = levels

        # key low bound
        self.key_low_bound = 0

        # key high bound
        self.key_high_bound = items - 1

        # fan out rate
        self.fan_out = fan_out

        # per node data capacity
        self.node_storage_capacity = math.ceil(items / pow(self.fan_out, self.levels))

        # root node
        self.root = self.build_tree(items, file_root, fp_prob)

    def build_tree(self, items, file_root, fp_prob):
        children = []
        child_cap = math.ceil(items/self.fan_out)
        child_range_low_bound = 1
        child_range_high_bound = child_cap

        for child in range(self.fan_out):
            filepath = file_root + '/lv-0.kr-' + str(child + 1) + '.cg-1'
            Path(filepath).mkdir(parents=True, exist_ok=True)
            node = Node(child_range_low_bound, child_range_high_bound, self.levels,
                        self.node_storage_capacity, 0, child + 1, 1, self.fan_out, filepath, fp_prob)
            children.append(node)
            child_range_low_bound = child_range_high_bound + 1
            child_range_high_bound = child_range_low_bound + child_cap - 1

        return MemBuf(self.node_storage_capacity, math.ceil(items/self.fan_out), children)

    def read(self, read_key, col_pos):
        return self.root.read(read_key, col_pos)

    def write(self, write_key, write_value):
        self.root.write(write_key, write_value)

