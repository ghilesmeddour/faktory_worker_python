import json
from typing import Dict, List

from .client import Client
from .models import Batch, Job
from .util import helper


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
        return self.client._push(job.dict(exclude_none=True))

    def push_bulk(self, jobs: List[Job]) -> Dict:
        msg = self.client._pushb([j.dict(exclude_none=True) for j in jobs])
        _, data = helper.RESP.parse_bulk_string(msg)
        return json.loads(data)

    def batch_new(self, batch: Batch) -> bool:
        return self.client._batch_new(batch.dict(exclude_none=True))

    def batch_open(self, bid: str) -> bool:
        return self.client._batch_open(bid)

    def batch_commit(self, bid: str) -> bool:
        return self.client._batch_commit(bid)
