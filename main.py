from value import Value
from lsmTree import LsmTree


def print_table(data):
    # Use a breakpoint in the code line below to debug your script.
    print('=========================================================')
    print('table_name: ' + data.name)
    print('primary key ' + '| col1     ' + '| col2     ' + '| col3     ' + '| col4')
    print(str(data.primarykey) + '           |' + data.col1 + '  |' + data.col2 + '  |' + data.col3 + '  |' + data.col4)
    print('=========================================================')


def print_value(key, val):
    if val:
        print('key: ' + str(key) + ', value: ' + Value.print_value(val))
    else:
        print('key: ' + str(key) + ', value: Not Found!')


def writes():
    lsm.write(25, Value('11', '12', '13', '14'))
    lsm.write(26, Value('21', '22', '23', '24'))
    lsm.write(44, Value('31', '32', '33', '34'))
    lsm.write(77, Value('41', '42', '43', '44'))
    lsm.write(12, Value('51', '52', '53', '54'))
    lsm.write(66, Value('61', '62', '63', '64'))
    lsm.write(82, Value('71', '72', '73', '74'))
    lsm.write(21, Value('81', '82', '83', '84'))
    lsm.write(96, Value('91', '92', '93', '94'))
    lsm.write(5, Value('01', '02', '03', '04'))


def reads():
    print_value(25, lsm.read(25, 0))
    print_value(66, lsm.read(66, 0))
    print_value(96, lsm.read(96, 0))
    print_value(28, lsm.read(28, 0))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    lsm = LsmTree(1000, 8, 4, '/Users/hollycasaletto/PycharmProjects/DeclStore/lsm', 0.05)

    writes()
    reads()

