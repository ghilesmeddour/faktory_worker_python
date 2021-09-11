# fproducer.py
import logging
import random
import time

from pyfaktory import Client, Producer

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

time.sleep(1)

with Client(faktory_url='tcp://localhost:7419', role='producer') as client:
    producer = Producer(client=client)
    while True:
        producer.push_job(jobtype='adder',
                          args=(random.randint(0,
                                               1000), random.randint(0, 1000)))
        time.sleep(1)
