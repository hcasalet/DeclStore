import math
import os
import os.path
from os import path
import pickle
from typing import Final
from membuf import MemBuf

# Configuration of the LSM:
#  1. Key range [l, h], number of columns (fields) in the table (object) is n.
#  2. With the root being the memory buffer, the LSM tree has another v levels going from level 0 to level v-1, with
#     a fan-out rate of f from level i to i+1 , and pow(2, i) column groups at level i.
#  3. The compaction from the memory buffer to level 0 is configured to be triggered when the key size in memory reaches
#     (h-l+1)/pow(f, v-1). In the process of Compaction from memory to level 0
#     will be triggered. In addition to the compaction from memory buffer to level 0, compaction always goes from a
#     parent page at level i to all its children pages at level i+1, when that parent page at level i gets full from
#     the compaction coming from its own parent at level i-1. So, in the process of compacting from memory buffer to
#     level 0, if any page in level 0 gets full, it triggers compaction from level 0 to level 1. This process continues
#     until level v-1 is reached, where data with any key value can be stored, and no more downward compaction is
#     needed.
#  5. Level i has pow(f, i+1) pages, and a column group number pow(2, i). Let p_j denote the jth page in level i, with
#     j being in the range of [1, pow(f, i+1)]. The key range for each page in level i will be:
#     [((p_j-1)*h + (pow(f, i+1) - (p_j-1))*l + (p_j-1)) / pow(f, i+1) * pow(2, i),
#      (p_j*h + (pow(f, i+1) - p)*l + p) / pow(f, i+1) * pow(2, i))
#  6. The page size at level i is (h-l+1)/pow(f, i+1)*pow(2, i)*Sigma(s_g), where g is the column group number and s_g
#     is the size of column group g. g ranges from 1 to pow(2, i), and fields for group g range from g*n/pow(2, i) to
#     (g+1)*n/pow(2, i)-1).


class LsmTree:
    PAGE_SIZE: Final = 10
    NUM_OF_COLS: Final = 4
    PAGE_GROWING_RATE: Final = 10

    def __init__(self, items, num_cols, objs_per_page):
        # number of levels is log[num_cols], starting at level 0 and thus +1
        self.levels = math.floor(math.log(num_cols, 2)) + 1

        # fan out rate = log[(items/objs_per_page) ** (1/levels)]
        self.fan_out = math.floor(math.log(math.ceil(math.pow(items/objs_per_page, (1/self.levels))), 2))

        # key range low bound always starts with 1, and high bound is 2*items
        self.key_low_bound = 1
        self.key_high_bound = 2 * items

        # number of columns
        self.num_of_cols = num_cols

        # root node
        self.root = MemBuf()



        self.buffer = {}
        self.buffer_size = 0
        self.buffer_capacity = cap
        self.fileroot = '/Users/hollycasaletto/PycharmProjects/DeclStore/lsm'
        self.level_0_size = 0
        self.level_1_size = 0
        self.level_0_pages = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}]
        self.level_1_pages = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}]
        self.level_2_pages = [{}, {}, {}, {}]

        self.level_0_page_key_range_cap = math.floor((self.max_key-self.min_key+1)/self.PAGE_GROWING_RATE)
        self.level_0_page_key_range_name_map = {}
        for i in range(self.PAGE_GROWING_RATE):
            self.level_0_page_key_range_name_map[i] = str(i*self.level_0_page_key_range_cap+1) + '-' + \
                                                      str((i+1)*self.level_0_page_key_range_cap)
        self.level_1_page_key_range_cap = \
            math.floor((self.max_key-self.min_key+1)/math.pow(self.PAGE_GROWING_RATE, 2))
        self.level_1_page_key_range_name_map = {}
        for i in range(math.floor(math.pow(self.PAGE_GROWING_RATE, 2))):
            self.level_1_page_key_range_name_map[i] = str(i*self.level_1_page_key_range_cap+1) + \
                                                 '-' + \
                                                 str((i+1)*self.level_1_page_key_range_cap)
        self.level_2_col_name_map = {}
        for i in range(self.NUM_OF_COLS):
            self.level_2_col_name_map[i] = 'col'+str(i+1)

    def read(self, read_cols, key):
        read_path = self.fileroot

        # ToDo: Need to check memory buffer and level_0 first

        if len(read_cols) == 1:
            range_factor = math.floor(key/(self.PAGE_SIZE*self.NUM_OF_COLS))
            read_path += '/level_2_' + read_cols[0] + '_' + str(range_factor*self.PAGE_SIZE*self.NUM_OF_COLS+1) + \
                         '-' + str((range_factor+1)*self.PAGE_SIZE*self.NUM_OF_COLS) + '.pickle'
            with open(read_path, 'rb') as infile:
                data = pickle.load(infile)
            infile.close()
            return data[key]
        else:
            range_factor = math.floor(key/self.PAGE_SIZE)
            read_path += '/level_1_' + str(range_factor*self.PAGE_SIZE+1) + '-' + \
                         str((range_factor+1)*self.PAGE_SIZE) + '.pickle'
            with open(read_path, 'rb') as infile:
                data = pickle.load(infile)
            infile.close()
            return(data[key])

    def write(self, val):
        if self.buffer_size < self.buffer_capacity:
            if val.primarykey not in self.buffer.keys():
                self.buffer[val.primarykey] = val
                self.buffer_size += 1

            if self.buffer_size >= self.buffer_capacity / 2:
                write_out = {}
                for key in self.buffer:
                    write_out[key] = self.buffer[key]
                self.buffer.clear()
                self.buffer_size = 0
                self.compact_buffer(write_out)
        else:
            print("Error: buffer is full")

    def compact_buffer(self, write_out):
        files_read = [False]*self.PAGE_GROWING_RATE*1
        for key in write_out:
            file_no = math.floor(key*self.PAGE_GROWING_RATE/(self.max_key-self.min_key+1))
            if not files_read[file_no]:
                if path.exists(self.fileroot+"/level_0_"+str(self.level_0_page_key_range_name_map[file_no])+".pickle"):
                    with open(self.fileroot+"/level_0_"+str(self.level_0_page_key_range_name_map[file_no])+".pickle", 'rb') as infile:
                        self.level_0_pages[file_no] = pickle.load(infile)
                    infile.close()
                files_read[file_no] = True

            if len(self.level_0_pages[file_no])+1 >= self.PAGE_SIZE:
                level_0_file = dict(self.level_0_pages[file_no])
                self.compact_level_0(level_0_file, file_no)
                self.level_0_pages[file_no].clear()
            self.level_0_pages[file_no][key] = write_out[key]

        for i in range(self.PAGE_GROWING_RATE):
            if self.level_0_pages[i]:
                if path.exists(self.fileroot+"/level_0_"+str(self.level_0_page_key_range_name_map[i])+".pickle"):
                    os.rename(self.fileroot + "/level_0_" + str(self.level_0_page_key_range_name_map[i]) + ".pickle",
                              self.fileroot + "/level_0_" + str(self.level_0_page_key_range_name_map[i]) + ".pickle.bak")
                with open(self.fileroot+"/level_0_"+str(self.level_0_page_key_range_name_map[i])+".pickle", 'wb') as outfile:
                    pickle.dump(self.level_0_pages[i], outfile, protocol=pickle.HIGHEST_PROTOCOL)
                outfile.close()
                self.level_0_pages[i].clear()

    def compact_level_0(self, level_0_file, order):
        col_file_no = math.floor(order / 2)
        if path.exists(self.fileroot + '/level_2_' + str(4 * col_file_no) + '.pickle'):
            with open(self.fileroot + '/level_2_' + str(4 * col_file_no) + '.pickle', 'rb') as infile:
                self.level_2_pages[0] = pickle.load(infile)
            infile.close()
        if path.exists(self.fileroot + '/level_2_' + str(4 * col_file_no+1) + '.pickle'):
            with open(self.fileroot + '/level_2_' + str(4 * col_file_no+1) + '.pickle', 'rb') as infile:
                self.level_2_pages[1] = pickle.load(infile)
            infile.close()
        if path.exists(self.fileroot + '/level_2_' + str(4 * col_file_no+2) + '.pickle'):
            with open(self.fileroot + '/level_2_' + str(4 * col_file_no+2) + '.pickle', 'rb') as infile:
                self.level_2_pages[2] = pickle.load(infile)
            infile.close()
        if path.exists(self.fileroot + '/level_2_' + str(4 * col_file_no+3) + '.pickle'):
            with open(self.fileroot + '/level_2_' + str(4 * col_file_no+3) + '.pickle', 'rb') as infile:
                self.level_2_pages[3] = pickle.load(infile)
            infile.close()

        files_1_read = [False]*self.PAGE_GROWING_RATE
        for key in level_0_file:
            file_no = math.floor(key/self.PAGE_SIZE)
            if not files_1_read[file_no]:
                if path.exists(self.fileroot+'/level_1_'+str(self.level_1_page_key_range_name_map[file_no])+'.pickle'):
                    with open(self.fileroot+'/level_1_'+str(self.level_1_page_key_range_name_map[file_no])+'.pickle', 'rb') as infile:
                        self.level_1_pages[file_no] = pickle.load(infile)
                    infile.close()
                files_1_read[file_no] = True
            self.level_1_pages[file_no][key] = level_0_file[key]

            self.level_2_pages[0][key] = level_0_file[key].col1
            self.level_2_pages[1][key] = level_0_file[key].col2
            self.level_2_pages[2][key] = level_0_file[key].col3
            self.level_2_pages[3][key] = level_0_file[key].col4

        for i in range(self.PAGE_GROWING_RATE):
            if self.level_1_pages[i]:
                if path.exists(self.fileroot+'/level_1_'+str(self.level_1_page_key_range_name_map[2*order+i])+'.pickle'):
                    os.rename(self.fileroot+'/level_1_'+str(self.level_1_page_key_range_name_map[2*order+i])+'.pickle',
                              self.fileroot+'/level_1_' + str(self.level_1_page_key_range_name_map[2*order+i])+'.pickle.bak')
                with open(self.fileroot+'/level_1_'+str(self.level_1_page_key_range_name_map[2*order+i])+'.pickle', 'wb') as outfile:
                    pickle.dump(self.level_1_pages[i], outfile, protocol=pickle.HIGHEST_PROTOCOL)
                outfile.close()
                self.level_1_pages[i].clear()

        key_capacity_per_page = math.floor((self.max_key - self.min_key + 1)/math.pow(self.PAGE_GROWING_RATE, 2))

        for i in range(4):
            if path.exists(self.fileroot + '/level_2_' + self.level_2_col_name_map[i] + '_' +
                           str(key_capacity_per_page*self.NUM_OF_COLS*col_file_no+1) + '-' +
                           str(key_capacity_per_page*self.NUM_OF_COLS*(col_file_no+1)) + '.pickle'):
                os.rename(self.fileroot + '/level_2_' + self.level_2_col_name_map[i] + '_' +
                          str(key_capacity_per_page*self.NUM_OF_COLS*col_file_no+1) + '-' +
                          str(key_capacity_per_page*self.NUM_OF_COLS*(col_file_no+1)) + '.pickle',
                          self.fileroot + '/level_2_' + self.level_2_col_name_map[i] + '_' +
                          str(key_capacity_per_page*self.NUM_OF_COLS*col_file_no+1)
                          + '-' + str(key_capacity_per_page*self.NUM_OF_COLS*(col_file_no+1)) + '.pickle.bak')

            with open(self.fileroot+'/level_2_'+ self.level_2_col_name_map[i] + '_' +
                      str(key_capacity_per_page*self.NUM_OF_COLS*col_file_no+1) + '-' +
                      str(key_capacity_per_page*self.NUM_OF_COLS*(col_file_no+1)) + '.pickle', 'wb') as outfile:
                pickle.dump(self.level_2_pages[i], outfile, protocol=pickle.HIGHEST_PROTOCOL)
            outfile.close()
            self.level_2_pages[i].clear()
