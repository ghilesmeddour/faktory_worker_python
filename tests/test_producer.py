import pytest

from pyfaktory import Client, Producer


class TestProducerConstructor:

    def test_init(self):
        with pytest.raises(ValueError):
            _ = Producer(client=Client(role='consumer'))

        _ = Producer(client=Client(role='producer'))
        _ = Producer(client=Client(role='both'))


class TestProducerMethods:

    def test_job_fields(self):
        required_fields = {'jid', 'jobtype', 'args'}
        optional_fields = {
            'queue', 'reserve_for', 'at', 'retry', 'backtrace', 'custom'
        }

        producer = Producer(client=Client(role='producer'))
        producer.client._push = lambda x: x

        job = producer.push_job(jid='somejobid', jobtype='somejobtype')

        assert required_fields.issubset(job)
        assert set(job.keys()).issubset(required_fields.union(optional_fields))

    def test_reserve_for_less_than_60(self):
        with pytest.raises(ValueError):
            producer = Producer(client=Client(role='producer'))
            producer.push_job(jid='somejobid',
                              jobtype='somejobtype',
                              reserve_for=59)
