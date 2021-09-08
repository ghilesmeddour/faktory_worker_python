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

    It fetches units of work (jobs) from the server and executes them.
    It retrieves the jobs from the queues, and decides how to execute them
    based on the jobtype.

    A handler must be attached to each jobtype before the consumer is launched.

    Parameters
    ----------
    client : pyfaktory.Client
        Faktory client. Client `role` must be either 'consumer' or 'both'.
    queues : List[str], default ['default']
        The queues from which the consumer will fetch jobs. If you provide
        no `queues`, the consumer will process `default` queue.
    priority : {{'strict', 'uniform', 'weighted'}}, default 'uniform'
        Priority indicates in which queue order the jobs should be fetched 
        first. With `strict` priority, the worker always fetches from the first 
        queue and will only try to fetch from the next once the previous queue 
        is empty. With `uniform`, each queue has an equal chance of being 
        fetched first. With `weighted`, queues have a different probability 
        of being fetched. These probabilities are specified with `weights` 
        argument.
    weights : Optional[List[float]], default None
        Probability of the queues to be fetched. This parameter is required 
        when `priority` is `weighted` (and ignored in other cases), and must 
        have the same number of elements as `queues`.
    concurrency : int, default 4
        Number of jobs to run at the same time.
    grace_period : int, default 25
        Grace period between the beginning of a shutdown and its end. 
        This period is used to give the job some time to finish, to stop them 
        properly and to notify the server. This period should never be longer 
        than 30 seconds.
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
