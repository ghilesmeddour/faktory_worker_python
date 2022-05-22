import uuid
from typing import Any, Dict, List, Optional

from pyfaktory.models import Job

from .client import Client
from .models import Job


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

    def push(self, job: Job) -> bool:
        return self.client._push(job.dict())

    def push_bulk(self, jobs: List[Job]) -> Dict:
        return self.client._pushb([j.dict() for j in jobs])

    # TODO
    def batch_new(self):
        pass

    # TODO
    def batch_open(self):
        pass

    # TODO
    def batch_commit(self):
        pass
