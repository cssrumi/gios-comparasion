import sys
from typing import Dict

import attr


@attr.s(frozen=True)
class Config:
    pgUser = attr.ib(type=str, kw_only=True)
    pgPassword = attr.ib(type=str, kw_only=True)
    pgHost = attr.ib(type=str, kw_only=True)
    pgPort = attr.ib(type=str, kw_only=True, converter=int)
    pgDB = attr.ib(type=str, kw_only=True)
    mongoUser = attr.ib(type=str, kw_only=True)
    mongoPassword = attr.ib(type=str, kw_only=True)
    mongoHost = attr.ib(type=str, kw_only=True)
    mongoPort = attr.ib(type=str, kw_only=True, converter=int)
    mongoDB = attr.ib(type=str, kw_only=True)
    mongoCollection = attr.ib(type=str, kw_only=True)


def get_kwargs() -> Dict[str, str]:
    args_list = [arg.split('=') for arg in sys.argv[1:]]
    args_dict = {k.lstrip('-'): v for k, v in args_list}
    return args_dict


_CONFIG = Config(**get_kwargs())


def get_config() -> Config:
    return _CONFIG
