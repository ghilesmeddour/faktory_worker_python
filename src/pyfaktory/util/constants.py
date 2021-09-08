####
## Client
####

CRLF = '\r\n'
DEFAULT_FAKTORY_URL = 'tcp://localhost:7419'
DEFAULT_PORT = 7419
DEFAULT_LABELS = ['python']
DEFAULT_QUEUES = ['default']

####
## Producer
####

RECOMMENDED_BEAT_PERIOD = 15  # seconds
MIN_ALLOOWABLE_BEAT_PERIOD = 5  # seconds
MAX_ALLOOWABLE_BEAT_PERIOD = 60  # seconds

####
## Consumer
####

DEFAULT_GRACE_PERIOD = 25
MAX_GRACE_PERIOD = 30
