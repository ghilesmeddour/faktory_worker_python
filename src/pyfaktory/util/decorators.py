from functools import wraps
from typing import List

from .enums import State
from .exceptions import FaktroyWorkProtocolError


def producer_cmd(fn):

    @wraps(fn)
    def wrapper(self, *args, **kw):
        if self.role == 'consumer':
            raise FaktroyWorkProtocolError(
                f'Trying to send producer command `{fn.__name__}` with a consumer client'
            )
        return fn(self, *args, **kw)

    return wrapper


def consumer_cmd(fn):

    @wraps(fn)
    def wrapper(self, *args, **kw):
        if self.role == 'producer':
            raise FaktroyWorkProtocolError(
                f'Trying to send consumer command `{fn.__name__}` with a producer client'
            )
        return fn(self, *args, **kw)

    return wrapper


def valid_states_cmd(states: List[State]):

    def decorator(fn):

        @wraps(fn)
        def wrapper(self, *args, **kw):
            if self.state not in states:
                raise FaktroyWorkProtocolError(
                    f'Client state is {self.state} while `{fn.__name__}` is only valid in the states {states}'
                )
            return fn(self, *args, **kw)

        return wrapper

    return decorator
