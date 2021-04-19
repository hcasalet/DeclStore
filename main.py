from value import value
from lsmTree import LsmTree

def print_table(data):
    # Use a breakpoint in the code line below to debug your script.
    print('=========================================================')
    print('table_name: ' + data.name)
    print('primary key ' + '| col1     ' + '| col2     ' + '| col3     ' + '| col4')
    print(str(data.primarykey) + '           |' + data.col1 + '  |' + data.col2 + '  |' + data.col3 + '  |' + data.col4)
    print('=========================================================')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    lsm = LsmTree(1, 10240, 256, 4, 8)
    obj = value('T1', 1, '11111111', '12121212', '13131313', '14141414')
    lsm.write(obj)

    obj = value('T1', 2, '21212121', '22222222', '23232323', '24242424')
    lsm.write(obj)

    obj = value('T1', 3, '31313131', '32323232', '33333333', '34343434')
    lsm.write(obj)

    obj = value('T1', 4, '41414141', '42424242', '43434343', '44444444')
    lsm.write(obj)

    obj = value('T1', 5, '51515151', '52525252', '53535353', '54545454')
    lsm.write(obj)

    obj = value('T1', 6, '61616161', '62626262', '63636363', '64646464')
    lsm.write(obj)

    obj = value('T1', 7, '71717171', '72727272', '73737373', '74747474')
    lsm.write(obj)

    obj = value('T1', 8, '81818181', '82828282', '83838383', '84848484')
    lsm.write(obj)

    obj = value('T1', 9, '91919191', '92929292', '93939393', '94949494')
    lsm.write(obj)

    obj = value('T1', 10, '10101010', '10210210', '10310310', '10410410')
    lsm.write(obj)

    obj = value('T1', 11, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    print_table(lsm.read(['col1', 'col2'], 9))

    print('col2 = ' + lsm.read(['col2'], 9))
    #print('col1 = ' + lsm.read(['col1'], 9))
    #print('col3 = ' + lsm.read(['col3'], 9))

''' 
    obj = value('T1', 68, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 37, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 7, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 5, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 4, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 6, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 8, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)

    obj = value('T1', 10, '24242424', '36363636', '48484848', '60606060')
    lsm.write(obj)
'''
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
