import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Extra, conint, validator
from rfc3339_validator import validate_rfc3339


class Job(BaseModel, extra=Extra.forbid):
    jid: str = None
    jobtype: str
    args: List[Any]
    queue: str = 'default'
    reserve_for: conint(ge=60) = 1800
    at: str = ''
    retry: conint(ge=-1) = 25
    backtrace: conint(ge=0) = 5
    custom: Optional[Dict] = None

    @validator('jid', pre=True, always=True)
    def set_random_jid(cls, jid):
        return jid or uuid.uuid4().hex

    @validator('at')
    def at_is_rfc3339(cls, at):
        if len(at) > 0 and not validate_rfc3339(at):
            raise ValueError(f'{at} is not RFC3339 valid')
        return at


class TargetJob(BaseModel, extra=Extra.forbid):
    jobtype: str
    args: List[Any]
    queue: str = 'default'


class Batch(BaseModel, extra=Extra.forbid):
    parent_bid: Optional[str]
    description: Optional[str]
    success: Optional[TargetJob]
    complete: Optional[TargetJob]


class JobFilter(BaseModel, extra=Extra.forbid):
    jids: Optional[List[str]]
    regexp: Optional[str]
    jobtype: Optional[str]


class Cmd(str, Enum):
    clear = 'clear'
    kill = 'kill'
    discard = 'discard'
    requeue = 'requeue'


class Target(str, Enum):
    retries = 'retries'
    scheduled = 'scheduled'
    dead = 'dead'


class MutateOperation(BaseModel):
    cmd: Cmd
    target: Target
    filter: Optional[JobFilter] = None

    class Config:
        use_enum_values = True
        extra = Extra.forbid
