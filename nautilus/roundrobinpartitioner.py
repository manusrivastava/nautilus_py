from kafka.partitioner.base import Partitioner
import sys

class RoundRobinPartitioner(Partitioner):
    def __init__(self, partitions):
        super(RoundRobinPartitioner, self).__init__(partitions)
        self.count = 0

    def partition(self, key, partitions):
        self.count = partitions[(self.count + 1) % len(partitions)]
        return self.count

