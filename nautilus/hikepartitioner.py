from kafka.partitioner.base import Partitioner
import sys
import mmh3

class HikePartitioner(Partitioner):
    HK_SEED = 0x1709e

    KEY_LIST = ["puc", "hud", "hau", "hul","pcs","pcu"]

    @staticmethod
    def get_partition(key):
        p = 0
        for k in HikePartitioner.KEY_LIST:
            if k == key:
                break
            p += 1
        return p

    def __init__(self, partitions):
        super(HikePartitioner, self).__init__(partitions)

    def partition(self, key, partitions=None):
        ret = HikePartitioner.get_partition(key)
        if ret == len(HikePartitioner.KEY_LIST):
            ret = (len(HikePartitioner.KEY_LIST) + mmh3.hash(key, HikePartitioner.HK_SEED)) % len(partitions);
        return ret


if __name__ == '__main__':
    h = HikePartitioner(1)
    print "%s => %d =>%d" %(sys.argv[1], h.partition(sys.argv[1]), mmh3.hash(sys.argv[1], HikePartitioner.HK_SEED))
