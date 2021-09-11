# fworker.py
import time
import logging

from pyfaktory import Client, Consumer

logging.basicConfig(level=logging.DEBUG)

time.sleep(1)


def adder(x, y):
    logging.info("%d + %d = %d", x, y, x + y)


with Client(faktory_url='tcp://localhost:7419', role='consumer') as client:
    consumer = Consumer(client=client, queues=['default'], concurrency=1)
    consumer.register('adder', adder)
    consumer.run()
