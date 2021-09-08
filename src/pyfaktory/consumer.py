from concurrent.futures.process import BrokenProcessPool
from typing import Callable, Dict, List, Optional
import multiprocessing
import traceback
import logging
import random
import signal
import math
import time
import sys

from pebble import ProcessPool, sighandler

from .client import Client
from .util.enums import State
from .util import constants as C
from .util import helper


class Consumer:
    """
    Faktory Consumer (Worker).

    Parameters
    ----------
    client : pyfaktory.Client
        Faktory Client.
    concurrency : int, default 1
        Number of processes to start.
    beat_period : int, default 15
        Lorem.
    """
    def __init__(self,
                 client: Client,
                 concurrency: int = 1,
                 beat_period=C.RECOMMENDED_BEAT_PERIOD) -> None:
        self.client = client
        self.concurrency = concurrency
        self.beat_period = beat_period
        self.executor = ProcessPoolExecutor(max_workers=self.concurrency)

        self.logger = logging.getLogger(name='FaktoryConsumer')

        self.labels = []
        self.pool = None
        self.state = ''
        self.queues = []
        self.done = None
        self.shutdown_waiter = None
        self.job_handlers: Dict[str, Callable] = {}
        self.event_handlers: Dict[LifecycleEventType, List[Callable]] = None
        self.weighted_priority_queues_enabled: bool = False
        self.weighted_queues = None
        pass

    def register(self, name: str, fn: Callable):
        """
        Register a handler for the given jobtype.

        It is expected that all jobtypes are registered upon process
        startup.
        """
        self.job_handlers[name] = fn
        self.logger.info(f"Registered handler for jobtype: {name}")

    def run(self):
        """
        """
        while True:
            pass
