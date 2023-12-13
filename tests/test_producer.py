import pytest

from pyfaktory import Client, Producer


class TestProducerConstructor:
    def test_init(self):
        with pytest.raises(ValueError):
            _ = Producer(client=Client(role="consumer"))

        _ = Producer(client=Client(role="producer"))
        _ = Producer(client=Client(role="both"))
