class FaktoryError(Exception):
    """
    Base Faktory Exception.
    """
    pass


class FaktroyWorkProtocolError(FaktoryError):
    """
    Faktroy Work Protocol Error.
    """
    pass


class FaktoryConsumerError(FaktoryError):
    """
    Faktory Worker Error.
    """
    pass
