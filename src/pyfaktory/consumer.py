import logging
import multiprocessing
import random
import signal
import sys
import time
import traceback
from typing import Callable, Dict, List, Optional

from pebble import ProcessPool, sighandler

from .client import Client
from .util import constants as C
from .util import helper
from .util.enums import State


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
    sentry_capture_exception : bool, default False
        If `True` capture exceptions using Sentry before failling jobs.
    """

    def __init__(self,
                 client: Client,
                 queues: List[str] = C.DEFAULT_QUEUES,
                 priority: str = 'uniform',
                 weights: Optional[List[float]] = None,
                 concurrency: int = 4,
                 grace_period: int = C.DEFAULT_GRACE_PERIOD,
                 sentry_capture_exception: bool = False) -> None:
        self.logger = logging.getLogger(name='FaktoryConsumer')

        if client.role == 'producer':
            raise ValueError(
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
            else:
                self.weights = weights

        self.concurrency = concurrency
        self.grace_period = grace_period
        self.sentry_capture_exception = sentry_capture_exception

        self.pool = ProcessPool(max_workers=self.concurrency)

        self.job_handlers: Dict[str, Callable] = {}

        self.pending_tasks_count = 0
        self.lock_pending_tasks_count = multiprocessing.Lock()

    @sighandler((signal.SIGTERM))
    def handle_sigterm(*_):
        raise KeyboardInterrupt

    def register(self, name: str, fn: Callable):
        """
        Register a handler for the given jobtype.

        It is expected that all jobtypes are registered upon process
        startup.
        """
        self.job_handlers[name] = fn
        self.logger.info(f"Registered handler for jobtype: {name}")

    def get_job_handler(self, name: str) -> Callable:
        try:
            return self.job_handlers[name]
        except KeyError:
            self.logger.error(f"'{name}' is not a registered job handler")
            # One could consider just failing the job and continue running,
            # but we are not doing it here because it is expected that all
            # jobtypes are registered upon process startup.
            raise ValueError(f"'{name}' has no registered handler")

    def get_queues(self) -> List[str]:
        if self.priority == 'strict':
            return self.queues
        elif self.priority == 'uniform':
            random.shuffle(self.queues)
            return self.queues
        else:
            return helper.weighted_shuffle(self.queues, self.weights)

    def task_done(self, future):
        self.logger.info(f'Task done callback called for job {future.job_id}')
        try:
            result = future.result()
            self.logger.info(f'Task (job {future.job_id}) returned {result}')
            self.client._ack(jid=future.job_id)
        except Exception as err:
            if self.sentry_capture_exception:
                import sentry_sdk
                sentry_sdk.capture_exception(err)

            err_type, err_value, err_traceback = sys.exc_info()
            self.logger.info(
                f'Task (job {future.job_id}) raised {err_type}: {err_value}')
            self.logger.debug(f'Task (job {future.job_id}) backtrace: ',
                              traceback.format_tb(err_traceback))
            self.client._fail(jid=future.job_id,
                              errtype=err_type.__name__,
                              message=str(err_value),
                              backtrace=traceback.format_tb(
                                  err_traceback, limit=future.backtrace))
        finally:
            with self.lock_pending_tasks_count:
                self.pending_tasks_count -= 1

    def run(self):
        """
        Start the consumer.

        When this method is called, the fetching and execution of the jobs
        starts. The job handlers must have been registered beforehand.

        This method is blocking, it only stops in the event of an error
        (only main loop errors, errors that occur in the handlers cause the job
        to fail and are reported to Faktory Server) or when a signal is received
        (Ctrl-C or from the Faktory Web UI).

        At the beginning of the shutdown, the worker gives itself a grace period
        to stop properly and notify the last information to the Faktory server.
        If a second signal is received, this causes an immediate shutdown.
        """
        self.logger.info('Entering run loop..')
        # TODO: check this
        while self.client.state in [State.IDENTIFIED, State.QUIET]:
            try:
                if self.client.state == State.QUIET:
                    self.logger.info(
                        f'State is {self.client.state}, not fetching further jobs'
                    )
                    time.sleep(self.client.beat_period)
                    continue

                if self.pending_tasks_count < self.concurrency:
                    queues_tmp = self.get_queues()
                    self.logger.info(f'Fetching from queues: {queues_tmp}')
                    # If no jobs are found, _fatch will block for up
                    # to 2 seconds on the first queue provided.
                    job = self.client._fetch(queues_tmp)

                    if job:
                        # TODO: check
                        # timeout=job.get('reserve_for', None)
                        job_handler = self.get_job_handler(job.get('jobtype'))

                        future = self.pool.schedule(job_handler,
                                                    args=job.get('args'))

                        with self.lock_pending_tasks_count:
                            self.pending_tasks_count += 1

                        future.job_id = job.get("jid")
                        future.backtrace = job.get("backtrace", 0)
                        future.add_done_callback(self.task_done)
                    else:
                        self.logger.debug('Queues are empty.')
                else:
                    # TODO: maybe use Event object instead of sleep
                    time.sleep(0.1)
            except KeyboardInterrupt:
                self.logger.info(
                    'First KeyboardInterrupt, stopping after grace period')
                break
            except Exception:
                self.logger.error(
                    f'Shutting down due to an error: {traceback.format_exc()}')
                break

        self.logger.info(f'Run loop exited, state is {self.client.state}')
        self.logger.info(f'Grace period of {self.grace_period} seconds...')
        self.logger.info(f'Press Ctrl-C again to stop immediately')

        try:
            self.pool.close()
            self.pool.join(timeout=self.grace_period)
        except KeyboardInterrupt:
            self.logger.info('Second KeyboardInterrupt, stopping immediately')

        self.logger.info(f'End of the grace period. Stopping.')
        sys.exit(1)


# TODO: set rss_kb (sys.getsizeof?)
