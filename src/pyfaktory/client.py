from typing import Dict, List, Optional
from urllib.parse import urlparse
import hashlib
import logging
import socket
import uuid
import json
import os

from .util import constants as C
from .util.exceptions import FaktroyWorkProtocolError
from .util.decorators import *
from .util.enums import State
from .util import helper


class Client:
    """
    Faktory Client.

    This is an implementation of FWP (Faktory Work Protocol) client. 
    
    FWP allows a client to interact with a Faktory work server. It permits 
    a client to authenticate to a Faktory server, submit units of work for 
    later execution, and/or fetch units of work for processing and subsequently 
    report their execution result.

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

    Notes
    -----
    `FWP specification <https://github.com/contribsys/faktory/blob/master/docs/protocol-specification.md>`_
    """
    def __init__(self,
                 faktory_url: Optional[str] = None,
                 role: str = 'producer',
                 timeout: Optional[int] = 30,
                 worker_id: Optional[str] = None,
                 labels: List[str] = C.DEFAULT_LABELS) -> None:
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

        self.sock = None
        self.timeout = timeout
        self.state = State.DISCONNECTED

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
            password_hash = str.encode(
                self.password) + str.encode(password_hash_salt)
            for _ in range(password_hash_iterations):
                password_hash = hashlib.sha256(password_hash).digest()
            password_hash = password_hash.hex()
        else:
            self.logger.info('Password is not required')

        _ = self._hello(pwdhash=password_hash)

        return True

    def disconnect(self):
        self.log.info("Disconnecting...")
        self.sock.close()
        self._set_state(State.DISCONNECTED)

    def _set_state(self, new_state: State) -> bool:
        if self.role == 'producer' and new_state in [
                State.QUIET, State.TERMINATING
        ]:
            raise FaktroyWorkProtocolError(
                f'Producer Client cannot enter {new_state} stage')

        old_state = self.state
        self.state = new_state
        self.logger.info(
            f'Client lifecycle state changed from {old_state} to {new_state}')
        return True

    def _send(self, command: str):
        self.sock.send(command.encode('utf-8'))
        self.logger.debug(f'C: {command}')

    def _receive(self) -> str:
        msg = self.sock.recv(1024).decode('utf-8')

        # Bulk Strings contain two CRLF
        if msg[0] == '$':
            nb_crlf = 2
        else:
            nb_crlf = 1

        while msg.count(C.CRLF) != nb_crlf:
            msg += self.sock.recv(1024).decode('utf-8')

        msg = msg.strip()
        self.logger.debug(f'S: {msg}')
        return msg

    def _send_and_receive(self, command: str) -> str:
        self._send(command)
        msg = self._receive()
        return msg

    def _raise_error(self, faktory_response):
        if faktory_response[0] == '-':
            raise FaktroyWorkProtocolError(
                f'Error received from Faktory server: {faktory_response}')

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
        client_info = {
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

        client_info = json.dumps(client_info)
        command = f'HELLO {client_info}{C.CRLF}'
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
        # TODO: complete FWD
        command = f'INFO{C.CRLF}'
        msg = self._send_and_receive(command)
        self._raise_error(msg)
        return msg

    @valid_states_cmd([State.IDENTIFIED, State.QUIET, State.TERMINATING])
    def _end(self) -> bool:
        """
        The END command is used to signal to the server that it wishes
        to terminate the connection.
        """
        command = f'END{C.CRLF}'
        # TODO: check OK response (Faktory Server PR)
        # msg = self._send_and_receive(command)
        # self._raise_error(msg)
        self._send(command)
        self._set_state(State.END)
        return True

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

    ####
    ## Consumer commands
    ####

    @consumer_cmd
    @valid_states_cmd([State.IDENTIFIED])
    def _fetch(self) -> Optional[Dict]:
        queues = []
        command = f'FETCH {json.dumps(queues)}{C.CRLF}'
        msg = self._send_and_receive(command)

        self._raise_error(msg)

        n_bytes, data = helper.parse_bulk_string(msg)

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
        args = {
            'wid': self.worker_id,
        }

        if self.state == State.QUIET:
            args['current_state'] == 'quiet'
        # TODO: check if this meet FWP
        elif self.state == State.TERMINATING:
            args['current_state'] == 'terminate'

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
            _, data = helper.parse_bulk_string(msg)
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