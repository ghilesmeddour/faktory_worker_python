import random

from . import constants as C


class RESP:
    """
    RESP (REdis Serialization Protocol).

    Notes
    -----
    `RESP <https://redis.io/topics/protocol>`_
    """

    @staticmethod
    def parse_bulk_string(s: str):
        assert s[0] == '$'

        try:
            n_bytes, data = s[1:].split('\r\n')
            return int(n_bytes), data
        except Exception:
            return -1, ''

    @staticmethod
    def is_message_complete(msg: str):
        if len(msg) < 2:
            return False

        if not msg.endswith(C.CRLF):
            return False

        if msg[0] == '$' and msg[1] != '-':
            nb_crlf = 2
        else:
            nb_crlf = 1

        return msg.count(C.CRLF) == nb_crlf


# http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf
def weighted_shuffle(items, weights):
    order = sorted(range(len(items)),
                   key=lambda i: random.random()**weights[i])
    return [items[i] for i in order]
