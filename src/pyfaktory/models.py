import uuid
from enum import Enum
from typing_extensions import Annotated
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict, conint, field_validator
from rfc3339_validator import validate_rfc3339


class Job(BaseModel, extra="forbid"):
    jid: str = Field(default_factory=lambda: uuid.uuid4().hex)
    jobtype: str
    args: List[Any]
    queue: str = 'default'
    reserve_for: Annotated[int, Field(ge=60)] = 1800
    at: str = ''
    retry: Annotated[int, Field(ge=-1)] = 25
    backtrace: Annotated[int, Field(ge=0)] = 5
    custom: Optional[Dict] = None

    @field_validator('at')
    def at_is_rfc3339(cls, at):
        if len(at) > 0 and not validate_rfc3339(at):
            raise ValueError(f'{at} is not RFC3339 valid')
        return at


class TargetJob(BaseModel, extra="forbid"):
    jobtype: str
    args: List[Any]
    queue: str = 'default'


class Batch(BaseModel, extra="forbid"):
    parent_bid: Optional[str]
    description: Optional[str]
    success: Optional[TargetJob]
    complete: Optional[TargetJob]


class JobFilter(BaseModel, extra="forbid"):
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
    model_config = ConfigDict(use_enum_values=True, extra='forbid')

    cmd: Cmd
    target: Target
    filter: Optional[JobFilter] = None
