import random


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
        n_bytes, data = s[1:].split('\r\n')
        return int(n_bytes), data


# http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf
def weighted_shuffle(items, weights):
    order = sorted(range(len(items)),
                   key=lambda i: random.random()**(1.0 / weights[i]))
    return [items[i] for i in order]
