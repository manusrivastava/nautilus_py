from .hikepartitioner import HikePartitioner
from .roundrobinpartitioner import RoundRobinPartitioner
from kafka import KeyedProducer, KafkaClient

class NautilusDive(object):
    def __init__(self, config):
        self.brokers = config['brokers']
        self.topic = config['topic']
        self.kafka = KafkaClient(self.brokers)
        if config['partitioner'] is None:
            self.producer = KeyedProducer(self.kafka, partitioner=RoundRobinPartitioner)
        else:
            self.producer = KeyedProducer(self.kafka, partitioner=config['partitioner'])

    def send(self, key, message):
        self.producer.send(self.topic, key, message)


if __name__ == '__main__':
    config = {}
    config['brokers'] = 'localhost:9092'
    config['topic'] = 'ht2'
    p = NautilusDive(config)
    msg = '{"name":"Varun"}'
    p.send('k', msg)
