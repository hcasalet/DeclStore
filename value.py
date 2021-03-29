import json


class value:
    """ data stored. Is the Value in the (key, value) pair """
    def __init__(self, table_name, a_int, b_str, c_str, d_str, e_str):
        self.name = table_name
        self.primarykey = a_int
        self.col1 = b_str
        self.col2 = c_str
        self.col3 = d_str
        self.col4 = e_str
