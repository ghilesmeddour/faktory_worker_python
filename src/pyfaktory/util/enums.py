from enum import Enum


class State(Enum):
    DISCONNECTED = 1
    NOT_IDENTIFIED = 2
    IDENTIFIED = 3
    QUIET = 4
    TERMINATING = 5
    END = 6
