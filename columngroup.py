import pickle


class ColumnGroup(object):
    def __init__(self, num_keys):
        self.num_keys = num_keys
        self.cg_map = dict()

    @staticmethod
    def get_column_groups_from_file(indata, start_len):
        cgmap_len = int.from_bytes(indata[start_len:(start_len + 4)], 'big')
        cgmap = pickle.loads(indata[(start_len + 4):(start_len + 4 + cgmap_len)])

        return cgmap, cgmap_len

    def prepare_column_group_map_to_write(self):
        cgmap_bytes = pickle.dumps(self)
        cgmap_length = len(cgmap_bytes)
        cgmap_len_bytes = cgmap_length.to_bytes(4, 'big')
        cgmap_with_len = cgmap_len_bytes + cgmap_bytes

        return cgmap_with_len
