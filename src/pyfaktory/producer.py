from typing import Any, Dict, List, Optional
import uuid

from .client import Client


class Producer:
    """
    Faktory Producer.

    Parameters
    ----------
    client : pyfaktory.Client
        Faktory client whose `role` must be either 'producer' or 'both'.
    """
    def __init__(self, client: Client) -> None:
        if client.role == 'consumer':
            raise ValueError(
                "Provided client is exclusively consumer and can't act as a producer"
            )
        self.client = client

    def push_job(self,
                 jobtype: str,
                 jid: Optional[str] = None,
                 args: List[Any] = [],
                 queue: str = 'default',
                 reserve_for: int = 1800,
                 at: str = '',
                 retry: int = 25,
                 backtrace: int = 5,
                 custom: Optional[Dict] = None) -> bool:
        """
        Push a job to Faktory server.

        Parameters
        ----------
        jid : str
            Globally unique ID for the job.
        jobtype : str
            Discriminator used by a worker to decide how to execute a job.
        args : List[Any]
            Parameters the worker should use when executing the job.
        queue : str, default 'default'
            Which job queue to push this job onto.
        reserve_for : int, default 1800
            Number of seconds a job may be held by a worker before
            it is considered failed (must be greater than 60).
        at : str, default ''
            Run the job at approximately this time, immediately if blank.
            RFC3339 string is expected, but no validation is done client side.
        retry : int, default 25
            Number of times to retry this job if it fails.
            0 discards the failed job, -1 saves the failed
            job to the dead set.
        backtrace : int, default 5
            Number of lines of FAIL information to preserve.
        custom : dict, default None
            Provides additional context to the worker executing the job.

        Returns
        -------
        bool
            Indicates if the push was successful.
        """
        if not jid:
            jid = uuid.uuid4().hex

        # Mandatory fields
        job = {
            'jid': jid,
            'jobtype': jobtype,
            'args': args,
        }

        if reserve_for < 60:
            raise ValueError(
                f'`reserve_for` == {reserve_for}, but must be greater than 60')

        # TODO: add `at` validation

        # Optional fields
        job.update({
            'queue': queue,
            'reserve_for': reserve_for,
            'at': at,
            'retry': retry,
            'backtrace': backtrace,
            'custom': custom,
        })

        # Optional fields can be omitted for some values.
        # TODO: omit fields when possible (this does not change anything,
        # it just allows to reduce the size of the command to send)

        return self.client._push(job)
