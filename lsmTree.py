import math
import os
import os.path
from os import path
import pickle
from typing import Final

# Design of the LSM:
#  1. Key range 1-10000
#  2. Levels: memory -> level 0 -> level 1 (final row level) -> level 2 (final column level)
#  3. Memory buffer size 2*100*data_unit. Alternating between the two. When one of them is full, switch to use the
#     other but the one that is full goes into compaction to level 0.
#  4. Level 0 has 1*10 pages each with the capacity of 100*data_unit. So they hold keys in the range of:
#     [1, 1000],
#     [1001, 2000],
#     [2001, 3000],
#     ......
#     [8001, 9000],
#     [9001, 10000]
#  5. Level 1 has 10*10 pages, each with the capacity of 100*data_unit. Whenever memory and level 0 merge, if the merge
#     generates more than what a level 0 page can hold, it will continue to compact into level 1 and level 2. The range
#     of keys that they hold are:
#     [1, 100],
#     [101, 200],
#     [201, 300],
#     ......,
#     [1001, 1100],
#     ......,
#     [9901, 10000].
#  6. Level 2 has 10*10 pages, and stores data in columnar format. each with the capacity of 1field*400rows.
#     The range of data contained in those pages are:
#     [col1, 1, 400],
#     [col2, 1, 400],
#     [col3, 1, 400],
#     [col4, 1, 400],
#     .......,
#     [col1, 9601, 10000],
#     [col2, 9601, 10000],
#     [col3, 9601, 10000],
#     [col4, 9601, 10000]


class LsmTree:
    PAGE_SIZE: Final = 10
    NUM_OF_COLS: Final = 4
    PAGE_GROWING_RATE: Final = 10

    def __init__(self, cap, minkey, maxkey):
        self.min_key = minkey
        self.max_key = maxkey
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
