# fworker.py
import logging
import time

from pyfaktory import Client, Consumer

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

time.sleep(1)


def adder(x, y):
    logging.info(f"{x} + {y} = {x + y}")


with Client(faktory_url='tcp://localhost:7419', role='consumer') as client:
    consumer = Consumer(client=client, queues=['default'], concurrency=1)
    consumer.register('adder', adder)
    consumer.run()
