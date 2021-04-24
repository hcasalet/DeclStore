from bloomfilter import BloomFilter
from random import shuffle
import pickle

n = 20  # no of items to add
p = 0.05  # false positive probability
filename = '/Users/hollycasaletto/PycharmProjects/DeclStore/lsm/try_holly_lsm_total.log'

# words to be added
word_present = ['abound', 'abounds', 'abundance', 'abundant', 'accessable',
                'bloom', 'blossom', 'bolster', 'bonny', 'bonus', 'bonuses',
                'coherent', 'cohesive', 'colorful', 'comely', 'comfort',
                'gems', 'generosity', 'generous', 'generously', 'genial']

# word not added
word_absent = ['bluff', 'cheater', 'hate', 'war', 'humanity',
               'racism', 'hurt', 'nuke', 'gloomy', 'facebook',
               'geeksforgeeks', 'twitter']


def write_bloom_filter():
    bloomf = BloomFilter(n, p)
    print("Size of bit array:{}".format(bloomf.size))
    print("False positive Probability:{}".format(bloomf.fp_prob))
    print("Number of hash functions:{}".format(bloomf.hash_count))

    for item in word_present:
        bloomf.add(item)

    with open(filename, "wb") as outfile:
        outfile.write(bloomf.prepare_bloom_filter_to_write())


def read_bloom_filter():
    bloomf = BloomFilter.read_bloom_filter_from_file(filename)

    shuffle(word_present)
    shuffle(word_absent)

    test_words = word_present[:10] + word_absent
    shuffle(test_words)
    for word in test_words:
        if bloomf.check(word):
            if word in word_absent:
                print("'{}' is a false positive!".format(word))
            else:
                print("'{}' is probably present!".format(word))
        else:
            print("'{}' is definitely not present!".format(word))


if __name__ == '__main__':
    write_bloom_filter()
    read_bloom_filter()
