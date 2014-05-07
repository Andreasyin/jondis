# -*- coding: utf-8 -*-
from redis import Redis
from .pool import Pool


def create_client(hosts):
    return Redis(connection_pool=Pool(hosts=hosts))
