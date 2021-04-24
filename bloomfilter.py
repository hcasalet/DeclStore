import math
import mmh3
import pickle
from bitarray import bitarray


class BloomFilter(object):
    def __init__(self, items_count, fp_prob):
        """
        items_count : int
            Number of items expected to be stored in bloom filter
        fp_prob : float
            False Positive probability in decimal
        """
        # False posible probability in decimal
        self.fp_prob = fp_prob

        # Size of bit array to use
        self.size = self.get_size(items_count, fp_prob)

        # number of hash functions to use
        self.hash_count = self.get_hash_count(self.size, items_count)

        # Bit array of given size
        self.bit_array = bitarray(self.size)

        # initialize all bits as 0
        self.bit_array.setall(0)

    def add(self, item):
        """
        Add an item in the filter
        """
        for i in range(self.hash_count):
            # create digest for given item.
            # i works as seed to mmh3.hash() function
            # With different seeds, digest created is different
            digest = mmh3.hash(item, i) % self.size

            # set the bit True in bit_array
            self.bit_array[digest] = True

    def check(self, item):
        """
        Check for existence of an item in filter
        """
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if not self.bit_array[digest]:
                # if any of bit is False then,its not present
                # in filter
                # else there is probability that it exist
                return False
        return True

    def prepare_bloom_filter_to_write(self):
        """
        With first 4 bytes as the length comes the serialized bloom filter
        ready to write to a file
        :return: bloom filter (with length) bytes
        """
        bloom_binary = pickle.dumps(self)
        length = len(bloom_binary)
        len_bytes = length.to_bytes(4, 'big')
        bloom_binary_with_len = len_bytes + bloom_binary
        return bloom_binary_with_len

    @staticmethod
    def read_bloom_filter_from_file(filename):
        """
        Given a file, builds the bloom filter in memory
        :param filename:
        :return: Bloom Filter object instance
        """
        with open(filename, "rb") as infile:
            length = int.from_bytes(infile.read(4), 'big')
            infile.seek(4)
            bf = infile.read(length)
            bloom = pickle.loads(bf)
        return bloom

    @classmethod
    def get_size(cls, n, p):
        """
        Return the size of bit array(m) to used using
        following formula
        m = -(n * lg(p)) / (lg(2)^2)
        n : int
            number of items expected to be stored in filter
        p : float
            False Positive probability in decimal
        """
        m = -(n * math.log(p))/(math.log(2)**2)
        return int(m)

    @classmethod
    def get_hash_count(cls, m, n):
        """
        Return the hash function(k) to be used using
        following formula
        k = (m/n) * lg(2)

        m : int
            size of bit array
        n : int
            number of items expected to be stored in filter
        """
        k = (m / n) * math.log(2)
        return int(k)
