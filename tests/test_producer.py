from unittest.mock import MagicMock

import pytest

from pyfaktory import Client, Producer
from pyfaktory.models import Job


class TestProducerConstructor:
    def test_init(self):
        with pytest.raises(ValueError):
            _ = Producer(client=Client(role="consumer"))

        _ = Producer(client=Client(role="producer"))
        _ = Producer(client=Client(role="both"))


class TestProducerPushBulk:
    def test_push_bulk_returns_empty_dict_on_null_bulk_string_stripped(self):
        """push_bulk returns {} when server returns $-1 (stripped, as returned by _receive)."""
        mock_client = MagicMock()
        mock_client.role = "producer"
        mock_client._pushb.return_value = "$-1"
        producer = Producer(client=mock_client)
        result = producer.push_bulk([Job(jobtype="MyJob", args=[])])
        assert result == {}

    def test_push_bulk_returns_empty_dict_on_null_bulk_string_with_crlf(self):
        """push_bulk returns {} when server returns $-1\r\n (unstripped null bulk string)."""
        mock_client = MagicMock()
        mock_client.role = "producer"
        mock_client._pushb.return_value = "$-1\r\n"
        producer = Producer(client=mock_client)
        result = producer.push_bulk([Job(jobtype="MyJob", args=[])])
        assert result == {}

    def test_push_bulk_returns_parsed_json_on_valid_response(self):
        """push_bulk parses and returns the JSON response normally."""
        mock_client = MagicMock()
        mock_client.role = "producer"
        mock_client._pushb.return_value = '$15\r\n{"jids":["abc"]}'
        producer = Producer(client=mock_client)
        result = producer.push_bulk([Job(jobtype="MyJob", args=[])])
        assert result == {"jids": ["abc"]}
