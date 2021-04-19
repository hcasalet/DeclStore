class Node:
    def __init__(self, i, j, k, p):
        self.level = i
        self.page_set = j
        self.col_group = k
        self.parent = p
        self.children = []