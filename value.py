import json


class Value:
    """ data stored. Is the Value in the (key, value) pair """
    def __init__(self, a_str, b_str, c_str, d_str):
        self.col1 = a_str
        self.col2 = b_str
        self.col3 = c_str
        self.col4 = d_str
        self.col5 = d_str + d_str
        self.col6 = c_str + c_str
        self.col7 = b_str + b_str
        self.col8 = a_str + a_str

    @staticmethod
    def print_value(val):
        return 'col1: ' + val.col1 + ', col2: ' + val.col2 + ', col3: ' + val.col3 + ', col4: ' + val.col4 \
               + ', col5: ' + val.col5 + ', col6: ' + val.col6 + ', col7: ' + val.col7 + ', col8: ' + val.col8

