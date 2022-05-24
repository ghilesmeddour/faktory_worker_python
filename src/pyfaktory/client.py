import hashlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .models import MutateOperation
from .util import constants as C
from .util import helper
from .util.decorators import *
from .util.enums import State
from .util.exceptions import FaktroyWorkProtocolError


class Client:
    """
    Faktory Client.

    This is an implementation of FWP (Faktory Work Protocol) client.

    FWP allows a client to interact with a Faktory work server. It permits
    a client to authenticate to a Faktory server, submit units of work (jobs)
    for later execution, and/or fetch units of work for processing and
    subsequently report their execution result.

    Parameters
    ----------
    faktory_url : Optional[str]
        URL of the Faktory server with which the client will establish
        a connection. Following format is expected: `tcp://:password@host:port`.
        If the url is not given, the client will try to find it
        in the environment variable `FAKTORY_URL`.
    role : {{'producer', 'consumer', 'both'}}, default 'both'
        The role of the client, some commands are only available for one role
        or another. For the client to act as both a consumer and a producer use
        `'both'`.
    timeout : Optional[int], default 30
        Timeout on blocking socket operations. If zero is given, the socket
        is put in non-blocking mode. If `None` is given, the socket is put
        in blocking mode.
    worker_id : Optional[str]
        Globally unique identifier for the worker that will use this client.
        If the client's role is `producer`, this argument is ignored.
    labels : List[str], default ['python']
        Labels that apply to the worker using this client. These labels
        will be displayed in Faktory webui.
        If the client's role is `producer`, this argument is ignored.
    beat_period : int, default 15
        The period in seconds for sending BEAT to the server to recognize state
        changes initiated by the server. Period must be between 5 and 60 seconds.

    Notes
    -----
    `FWP specification <https://github.com/contribsys/faktory/blob/master/docs/protocol-specification.md>`_
    """

    def __init__(self,
                 faktory_url: Optional[str] = None,
                 role: str = 'producer',
                 timeout: Optional[int] = 30,
                 worker_id: Optional[str] = None,
                 labels: List[str] = C.DEFAULT_LABELS,
                 beat_period: int = C.RECOMMENDED_BEAT_PERIOD) -> None:
        self.logger = logging.getLogger(name='FaktoryClient')

        if role not in ['consumer', 'producer', 'both']:
            raise ValueError(
                f"Unexpected role ({role}), role should be 'consumer', 'producer' or 'both'"
            )
        self.role = role

        if not faktory_url:
            faktory_url = os.environ.get('FAKTORY_URL', C.DEFAULT_FAKTORY_URL)

        parsed_url = urlparse(faktory_url)
        self.host = parsed_url.hostname
        self.port = parsed_url.port or C.DEFAULT_PORT
        self.password = parsed_url.password

        self.sock: socket.socket
        self.timeout = timeout
        self.state = State.DISCONNECTED

        self.rlock = threading.RLock()

        # Consumer specific fields
        if self.role != 'producer':
            self.labels = labels

            if worker_id is None:
                self.logger.warning(
                    f'No worker id has been given, a random id will be used')
            elif len(worker_id) < 8:
                raise ValueError(
                    'Worker id must be a string of at least 8 characters')

            self.worker_id = worker_id or uuid.uuid4().hex

            if not (C.MIN_ALLOOWABLE_BEAT_PERIOD <= beat_period <=
                    C.MAX_ALLOOWABLE_BEAT_PERIOD):
                ValueError(
                    'Beat period is {beat_period}, but should be between {C.MIN_ALLOOWABLE_BEAT_PERIOD} and {C.MAX_ALLOOWABLE_BEAT_PERIOD}'
                )
            self.beat_period = beat_period
            self.rss_kb = None
            self.heartbeat_thread: threading.Thread

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    @valid_states_cmd([State.DISCONNECTED])
    def connect(self) -> bool:
        self.logger.info(f'Client lifecycle state is {self.state}')

        self.logger.info('Openning connection...')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

        # The very first message from server
        msg = self._receive()

        self._raise_error(msg)

        self.logger.info('Connection to server is successfully established')
        self._set_state(State.NOT_IDENTIFIED)

        msg_args = json.loads(msg[3:])
        protocol_version = int(msg_args["v"])
        if protocol_version != 2:
            raise FaktroyWorkProtocolError(
                f'Only FWP version 2 supported, got {protocol_version}')

        password_hash = None
        if 'i' in msg_args and 's' in msg_args:
            self.logger.info('Password is required')
            if not self.password:
                raise ValueError('Password required but not provided')
            password_hash_iterations = msg_args['i']
            password_hash_salt = msg_args['s']
            self.logger.info('Hashing password...')
            password_hash = str.encode(str(
                self.password)) + str.encode(password_hash_salt)
            for _ in range(password_hash_iterations):
                password_hash = hashlib.sha256(password_hash).digest()
        else:
            self.logger.info('Password is not required')

        if password_hash:
            _ = self._hello(pwdhash=password_hash.hex())
        else:
            _ = self._hello()

        return True

    def disconnect(self):
        if self.state != State.END:
            with self.rlock:
                self._end()

        self.logger.info("Disconnecting...")
        self.sock.close()
        self._set_state(State.DISCONNECTED)

    def mutate(self, operation: MutateOperation) -> bool:
        return self._mutate(operation.dict(exclude_none=True))

    def info(self) -> Dict:
        msg = self._info()
        _, data = helper.RESP.parse_bulk_string(msg)
        return json.loads(data)

    def _set_state(self, new_state: State) -> bool:
        if self.role == 'producer' and new_state in [
                State.QUIET, State.TERMINATING
        ]:
            raise FaktroyWorkProtocolError(
                f'Producer Client cannot enter {new_state} stage')

        old_state = self.state

        if old_state == new_state:
            self.logger.info(f'client state is {old_state}, not changed')
            return False

        self.state = new_state
        self.logger.info(
            f'Client state changed from {old_state} to {new_state}')

        if self.role != 'producer':
            # If client acts as consumer, start heartbeating
            # when IDENTIFIED state is entered
            if self.state == State.IDENTIFIED:
                self.heartbeat_thread = threading.Thread(
                    target=self._heartbeat, args=())
                self.heartbeat_thread.start()

        return True

    def _send(self, command: str):
        self.sock.send(command.encode('utf-8'))
        self.logger.debug(f'C: {command}')

    def _receive(self) -> str:
        msg = self.sock.recv(1024).decode('utf-8')

        while not helper.RESP.is_message_complete(msg):
            msg += self.sock.recv(1024).decode('utf-8')

        msg = msg.strip()
        self.logger.debug(f'S: {msg}')
        return msg

    def _send_and_receive(self, command: str) -> str:
        with self.rlock:
            self._send(command)
            msg = self._receive()
            return msg

    def _raise_error(self, faktory_response):
        if faktory_response[0] == '-':
            raise FaktroyWorkProtocolError(
                f'Error received from Faktory server: {faktory_response}')

    def _heartbeat(self):
        while self.state in [State.IDENTIFIED, State.QUIET]:
            with self.rlock:
                self.logger.info(
                    f'Sending heartbeat to server, next heartbeat in {self.beat_period} seconds'
                )
                self._beat(rss_kb=self.rss_kb)
            time.sleep(self.beat_period)

    ####
    ## Client commands
    ####

    @valid_states_cmd([State.NOT_IDENTIFIED])
    def _hello(self, pwdhash=None) -> bool:
        """
        The HELLO command MUST be the first command issued by any client when
        connecting to a Faktory server. It is sent in response to the server's
        initial HI message.
        """
        client_info: Dict[str, Any] = {
            # Protocol version number
            "v": 2,
        }

        # Required fields for consumers
        if self.role != 'producer':
            client_info.update({
                "hostname": self.host,
                "wid": self.worker_id,
                "pid": os.getpid(),
                "labels": self.labels,
            })

        # Required fields for protected server
        if pwdhash:
            client_info['pwdhash'] = pwdhash

        command = f'HELLO {json.dumps(client_info)}{C.CRLF}'
        msg = self._send_and_receive(command)

        self._raise_error(msg)
        self._set_state(State.IDENTIFIED)

        return True

    @valid_states_cmd([State.IDENTIFIED])
    def _flush(self) -> bool:
        """
        FLUSH allows to clear all info from Faktory's internal database.
        It uses Redis's FLUSHDB command under the covers.
        """
        command = f'FLUSH{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @valid_states_cmd([State.IDENTIFIED])
    def _info(self) -> str:
        command = f'INFO{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return msg

    @valid_states_cmd([State.IDENTIFIED, State.QUIET, State.TERMINATING])
    def _end(self):
        """
        The END command is used to signal to the server that it wishes
        to terminate the connection.
        """
        command = f'END{C.CRLF}'
        self._send(command)
        self._set_state(State.END)

    @valid_states_cmd([State.IDENTIFIED])
    def _mutate(self, operation: Dict) -> bool:
        op = json.dumps(operation, separators=(',', ':'))
        command = f'MUTATE {op}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @valid_states_cmd([State.IDENTIFIED])
    def _batch_status(self, bid: str) -> str:
        command = f'BATCH STATUS {bid}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return msg

    ####
    ## Producer commands
    ####

    @producer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _push(self, work_unit: Dict) -> bool:
        command = f'PUSH {json.dumps(work_unit)}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @producer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _pushb(self, work_units: List[Dict]) -> str:
        command = f'PUSHB {json.dumps(work_units)}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return msg

    @producer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _batch_new(self, batch: Dict) -> str:
        command = f'BATCH NEW {json.dumps(batch)}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return msg

    @producer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _batch_commit(self, bid: str) -> bool:
        command = f'BATCH COMMIT {bid}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @producer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _batch_open(self, bid: str) -> bool:
        command = f'BATCH OPEN {bid}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    ####
    ## Consumer commands
    ####

    @consumer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _fetch(self, queues: List[str] = []) -> Optional[Dict]:
        command = f'FETCH {" ".join(queues)}{C.CRLF}'
        msg = self._send_and_receive(command)

        self._raise_error(msg)

        n_bytes, data = helper.RESP.parse_bulk_string(msg)

        if n_bytes == -1:
            return None
        else:
            return json.loads(data)

    @consumer_cmd
    @valid_states_cmd([State.IDENTIFIED, State.QUIET, State.TERMINATING])
    def _ack(self, jid: str) -> bool:
        args = {'jid': jid}
        command = f'ACK {json.dumps(args)}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @consumer_cmd
    @valid_states_cmd([State.IDENTIFIED, State.QUIET, State.TERMINATING])
    def _fail(self, jid: str, errtype: str, message: str,
              backtrace: List[str]) -> bool:
        args = {
            'jid': jid,
            'errtype': errtype,
            'message': message,
            'backtrace': backtrace,
        }
        command = f'FAIL {json.dumps(args)}{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return True

    @consumer_cmd
    @valid_states_cmd([State.IDENTIFIED, State.QUIET])
    def _beat(self, rss_kb: Optional[int] = None) -> bool:
        args: Dict[str, Any] = {
            'wid': self.worker_id,
        }

        if self.state == State.QUIET:
            args['current_state'] = 'quiet'
        # TODO: check if this meet FWP
        elif self.state == State.TERMINATING:
            args['current_state'] = 'terminate'

        if rss_kb:
            args['rss_kb'] = rss_kb

        command = f'BEAT {json.dumps(args)}{C.CRLF}'
        msg = self._send_and_receive(command)

        self._raise_error(msg)

        if msg[0] == '+':
            if msg[1:3] == 'OK':
                return True
        # Bulk String
        elif msg[0] == '$':
            _, data = helper.RESP.parse_bulk_string(msg)
            data = json.loads(data)
            if data['state'] == 'quiet':
                self._set_state(State.QUIET)
            elif data['state'] == 'terminate':
                self._set_state(State.TERMINATING)
            else:
                raise FaktroyWorkProtocolError(
                    f'Unexpected BEAT response state ({data})')
        else:
            raise FaktroyWorkProtocolError(
                f'Unexpected BEAT response: ({msg})')

        return True
