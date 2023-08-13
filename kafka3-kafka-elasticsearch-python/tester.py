import json
import logging

from utils.kafka_handler import KafkaHandler
from kafka import KafkaProducer


def run_it(logger=None):
    logger = logging.getLogger(__name__)
    # enable the debug logger if you want to see ALL of the lines
    #logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)

    kh = KafkaHandler(['localhost:9092'], 'tracking-log')
    logger.addHandler(kh)

    logger.info("INFO 레벨의 Log")
    logger.debug("DEBUG 레벨의 Log")


if __name__ == "__main__":
    run_it()