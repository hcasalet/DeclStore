import json


class Value:
    """ data stored. Is the Value in the (key, value) pair """
    '''
    def __init__(self, a_str, b_str, c_str, d_str):
        self.col1 = a_str
        self.col2 = b_str
        self.col3 = c_str
        self.col4 = d_str
        self.col5 = d_str + d_str
        self.col6 = c_str + c_str
        self.col7 = b_str + b_str
        self.col8 = a_str + a_str
    '''

    def __init__(self, cols, length):
        self.cols = cols
        self.num_cols = length

    @staticmethod
    def print_value(val):
        pstr = ''
        i = 0
        for f, s in val.cols:
            if i == 0:
                pstr += 'col' + str(f) + ': ' + s
                i += 1
            else:
                pstr += ', col' + str(f) + ': ' + s

        return pstr

