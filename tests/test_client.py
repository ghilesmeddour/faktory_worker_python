from pyfaktory import Client


class TestClientConstructor:

    def test_init(self):
        faktory_url = 'tcp://my-server:7419'
        role = 'both'
        timeout = 30
        worker_id = 'someworkerid'
        labels = ['foo', 'bar']
        beat_period = 15

        client = Client(faktory_url=faktory_url,
                        role=role,
                        timeout=timeout,
                        worker_id=worker_id,
                        labels=labels,
                        beat_period=beat_period)

        assert client.host == 'my-server'
        assert client.port == 7419
        assert client.scheme == 'tcp'
        assert client.password is None
        assert client.role == 'both'
        assert client.timeout == timeout
        assert client.worker_id == worker_id
        assert client.labels == labels
        assert client.beat_period == beat_period

    def test_use_env_faktory_url(self, monkeypatch):
        env_faktory_url = 'tcp://env-server:7400'
        monkeypatch.setenv('FAKTORY_URL', env_faktory_url)

        client = Client()

        assert client.host == 'env-server'
        assert client.port == 7400
        assert client.scheme == 'tcp'

    def test_ignore_env_faktory_url(self, monkeypatch):
        env_faktory_url = 'tcp://env-server:7400'
        monkeypatch.setenv('FAKTORY_URL', env_faktory_url)

        client = Client(faktory_url='tcp://arg-server:7411')

        assert client.host == 'arg-server'
        assert client.port == 7411
        assert client.scheme == 'tcp'

    def test_use_default_faktory_url(self, monkeypatch):
        monkeypatch.delenv('FAKTORY_URL', raising=False)

        client = Client()

        assert client.host == 'localhost'
        assert client.port == 7419
        assert client.scheme == 'tcp'

    def test_set_password(self):
        client = Client(faktory_url='tcp://:sdg145sd@q!@@foo-server:7419')

        assert client.host == 'foo-server'
        assert client.port == 7419
        assert client.scheme == 'tcp'
        assert client.password == 'sdg145sd@q!@'
    
    def test_use_tcp_tls(self):
        client = Client(faktory_url='tcp+tls://foo-server:7419')

        assert client.host == 'foo-server'
        assert client.port == 7419
        assert client.scheme == 'tcp+tls'

    def test_consumer_specific_fields(self):
        consumer_specific_fields = {
            'labels', 'worker_id', 'beat_period', 'rss_kb'
        }

        client_both = Client(role='both')
        client_consumer = Client(role='consumer')
        client_producer = Client(role='producer')

        print(client_both.__dict__.keys())
        print()
        print(consumer_specific_fields)
        assert consumer_specific_fields.issubset(client_both.__dict__)
        assert consumer_specific_fields.issubset(client_consumer.__dict__)
        assert not consumer_specific_fields.issubset(client_producer.__dict__)
