import random

import pytest
import numpy as np
import xarray as xr

from pyfaktory import Client, Consumer


class TestConsumerConstructor:
    def test_init(self):
        with pytest.raises(ValueError):
            _ = Consumer(client=Client(role='producer'))

        _ = Consumer(client=Client(role='consumer'))
        _ = Consumer(client=Client(role='both'))

    # TODO: add more unit tests


class TestConsumerGetQueues:
    def test_get_queues_strict(self):
        consumer = Consumer(client=Client(role='consumer'),
                            priority='strict',
                            queues=['a', 'b', 'c'])

        for _ in range(10000):
            assert consumer.get_queues() == ['a', 'b', 'c']

    def test_get_queues_uniform(self):
        consumer = Consumer(client=Client(role='consumer'),
                            priority='uniform',
                            queues=['a', 'b', 'c'])

        count_table = xr.DataArray(np.zeros((3, 3)),
                                   dims=("queue", "count"),
                                   coords={
                                       'queue': ['a', 'b', 'c'],
                                       'count': [0, 1, 2]
                                   })

        size = 10000
        random.seed(0)
        for _ in range(size):
            queues = consumer.get_queues()
            for i, q in enumerate(queues):
                count_table.loc[q, i] += 1

        assert (np.abs(count_table / size - 1 / 3) < 0.01).all()

    def test_get_queues_weighted(self):
        consumer = Consumer(client=Client(role='consumer'),
                            priority='weighted',
                            queues=['a', 'b', 'c'],
                            weights=[0.5, 0.3, 0.2])

        count_table = xr.DataArray(np.zeros((3, 3)),
                                   dims=("queue", "count"),
                                   coords={
                                       'queue': ['a', 'b', 'c'],
                                       'count': [0, 1, 2]
                                   })

        size = 10000
        random.seed(0)
        for _ in range(size):
            queues = consumer.get_queues()
            for i, q in enumerate(queues):
                count_table.loc[q, i] += 1

        count_table /= size

        assert count_table.loc['a', 0] - 0.5 < 0.1
        assert count_table.loc['b', 0] - 0.3 < 0.1
        assert count_table.loc['c', 0] - 0.2 < 0.1


# TODO: add more unit tests
