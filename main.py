from value import Value
from lsmTree import LsmTree

#  TODO: add a column-list in the paremeter list, so when compact from level i to level i+1, columns also split;
#        also, need to think of a way to construct "Value" with different number of columns.

#  TODO: Questions:
#  1. What happens if the initial estimate of the storage size requirement was very off? What is the cost of
#     the re-configuration?
#  2. key? How to design keys?


def print_value(key, val):
    if val:
        print('key: ' + str(key) + ', value: ' + Value.print_value(val))
    else:
        print('key: ' + str(key) + ', value: Not Found!')


def writes():
    lsm.write(25, Value([(1, '11'), (2, '12'), (3, '13'), (4, '14')], 4))
    lsm.write(26, Value([(1, '21'), (2, '22'), (3, '23'), (4, '24')], 4))
    lsm.write(44, Value([(1, '31'), (2, '32'), (3, '33'), (4, '34')], 4))
    lsm.write(77, Value([(1, '41'), (2, '42'), (3, '43'), (4, '44')], 4))
    lsm.write(12, Value([(1, '51'), (2, '52'), (3, '53'), (4, '54')], 4))
    lsm.write(66, Value([(1, '61'), (2, '62'), (3, '63'), (4, '64')], 4))
    lsm.write(82, Value([(1, '71'), (2, '72'), (3, '73'), (4, '74')], 4))
    lsm.write(21, Value([(1, '81'), (2, '82'), (3, '83'), (4, '84')], 4))
    lsm.write(96, Value([(1, '91'), (2, '92'), (3, '93'), (4, '94')], 4))
    lsm.write(5, Value([(1, '01'), (2, '02'), (3, '03'), (4, '04')], 4))
    lsm.write(15, Value([(1, '12'), (2, '22'), (3, '32'), (4, '42')], 4))


def reads():
    print_value(25, lsm.read(25, 25, 0))
    print_value(66, lsm.read(66, 66, 0))
    print_value(96, lsm.read(96, 96, 0))
    print_value(15, lsm.read(15, 15, 0))
    print_value(28, lsm.read(28, 28, 0))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    lsm = LsmTree(10000, 3, 10, '/Users/hollycasaletto/PycharmProjects/DeclStore/lsm', 0.05)

    writes()
    reads()

