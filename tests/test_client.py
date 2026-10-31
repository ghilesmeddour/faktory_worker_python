import json
import socket
from unittest.mock import MagicMock

import pytest

from pyfaktory import Client
from pyfaktory.util.enums import State


class TestClientConstructor:
    def test_init(self):
        faktory_url = "tcp://my-server:7419"
        role = "both"
        timeout = 30
        worker_id = "someworkerid"
        labels = ["foo", "bar"]
        beat_period = 15

        client = Client(
            faktory_url=faktory_url,
            role=role,
            timeout=timeout,
            worker_id=worker_id,
            labels=labels,
            beat_period=beat_period,
        )

        assert client.host == "my-server"
        assert client.port == 7419
        assert client.scheme == "tcp"
        assert client.password is None
        assert client.role == "both"
        assert client.timeout == timeout
        assert client.worker_id == worker_id
        assert client.labels == labels
        assert client.beat_period == beat_period

    def test_use_env_faktory_url(self, monkeypatch):
        env_faktory_url = "tcp://env-server:7400"
        monkeypatch.setenv("FAKTORY_URL", env_faktory_url)

        client = Client()

        assert client.host == "env-server"
        assert client.port == 7400
        assert client.scheme == "tcp"

    def test_ignore_env_faktory_url(self, monkeypatch):
        env_faktory_url = "tcp://env-server:7400"
        monkeypatch.setenv("FAKTORY_URL", env_faktory_url)

        client = Client(faktory_url="tcp://arg-server:7411")

        assert client.host == "arg-server"
        assert client.port == 7411
        assert client.scheme == "tcp"

    def test_use_default_faktory_url(self, monkeypatch):
        monkeypatch.delenv("FAKTORY_URL", raising=False)

        client = Client()

        assert client.host == "localhost"
        assert client.port == 7419
        assert client.scheme == "tcp"

    def test_set_password(self):
        client = Client(faktory_url="tcp://:sdg145sd@q!@@foo-server:7419")

        assert client.host == "foo-server"
        assert client.port == 7419
        assert client.scheme == "tcp"
        assert client.password == "sdg145sd@q!@"

    def test_use_tcp_tls(self):
        client = Client(faktory_url="tcp+tls://foo-server:7419")

        assert client.host == "foo-server"
        assert client.port == 7419
        assert client.scheme == "tcp+tls"

    def test_consumer_specific_fields(self):
        consumer_specific_fields = {
            "labels",
            "worker_id",
            "beat_period",
            "rss_kb",
            "hostname",
        }

        client_both = Client(role="both")
        client_consumer = Client(role="consumer")
        client_producer = Client(role="producer")

        assert consumer_specific_fields.issubset(client_both.__dict__)
        assert consumer_specific_fields.issubset(client_consumer.__dict__)
        assert not consumer_specific_fields.issubset(client_producer.__dict__)

    @pytest.mark.parametrize("beat_period", [4, 61])
    def test_invalid_beat_period(self, beat_period):
        with pytest.raises(ValueError):
            Client(role="consumer", beat_period=beat_period)

    def test_default_hostname(self):
        client = Client(role="consumer")

        assert client.hostname == socket.gethostname()

    def test_custom_hostname(self):
        client = Client(role="consumer", hostname="my-worker")

        assert client.hostname == "my-worker"


class TestClientHello:
    def test_hello_sends_client_hostname(self):
        client = Client(
            faktory_url="tcp://my-server:7419", role="consumer", hostname="my-worker"
        )
        client.state = State.NOT_IDENTIFIED
        client._send_and_receive = MagicMock(return_value="+OK")
        client._set_state = MagicMock()

        client._hello()

        command = client._send_and_receive.call_args.args[0]
        assert command.startswith("HELLO ")
        assert json.loads(command[len("HELLO ") :])["hostname"] == "my-worker"


class TestClientQueueCommands:
    @pytest.mark.parametrize("method", ["queue_remove", "queue_pause", "queue_unpause"])
    @pytest.mark.parametrize("queues", [None, []])
    def test_raise_without_queues(self, method, queues):
        client = Client()
        client.state = State.IDENTIFIED
        client._send_and_receive = MagicMock()

        with pytest.raises(ValueError):
            getattr(client, method)(queues=queues)
        client._send_and_receive.assert_not_called()

    @pytest.mark.parametrize(
        "method,verb",
        [
            ("queue_remove", "REMOVE"),
            ("queue_pause", "PAUSE"),
            ("queue_unpause", "RESUME"),
        ],
    )
    def test_send_queues(self, method, verb):
        client = Client()
        client.state = State.IDENTIFIED
        client._send_and_receive = MagicMock(return_value="+OK")

        getattr(client, method)(queues=["a", "b"])
        getattr(client, method)(all_queues=True)

        commands = [c.args[0] for c in client._send_and_receive.call_args_list]
        assert commands == [f"QUEUE {verb} a b\r\n", f"QUEUE {verb} *\r\n"]
