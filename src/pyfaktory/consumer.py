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
                 queues: List[str] = C.DEFAULT_QUEUES,
                 priority: str = 'uniform',
                 weights: Optional[List[float]] = None,
                 concurrency: int = 4,
                 grace_period: int = C.DEFAULT_GRACE_PERIOD) -> None:
        self.logger = logging.getLogger(name='FaktoryConsumer')

        if client.role == 'producer':
            raise Exception(
                "Provided client is exclusively producer and can't act as a consumer"
            )
        self.client = client

        self.queues = queues

        if priority not in ['strict', 'uniform', 'weighted']:
            raise ValueError(
                f"Unexpected priority ({priority}), priority should be 'strict', 'uniform' or 'weighted'"
            )
        self.priority = priority

        if self.priority == 'weighted':
            if weights is None:
                raise ValueError(
                    'Priority is weighted but weights are not provided')
            elif len(weights) != len(self.queues):
                raise ValueError('Weights and queues lengths mismatch')
            elif abs(math.fsum(weights) - 1.) > 0.0001:
                raise ValueError('Weights do not sum to 1')
            else:
                self.weights = weights

        self.concurrency = concurrency
        self.grace_period = grace_period
        
        self.pool = ProcessPool(max_workers=self.concurrency)
        
        self.job_handlers: Dict[str, Callable] = {}

        self.pending_tasks_count = 0
        self.lock_pending_tasks_count = multiprocessing.Lock()

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
