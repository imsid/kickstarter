#!/usr/bin/py


from snakebite.client import Client
from os import path
import json

default_path = '/user/hive/warehouse'


class HDFSStat(object):

    cluster = 'hostname'
    port = 8020
    default_path = '/user/hive/warehouse'

    @staticmethod
    def build_path(table):
        nm = table.split('.')[0]
        tb = table.split('.')[1]
        return default_path + '/' + nm + '.db/' + tb

    def __init__(self):
        self.client = Client(HDFSStat.cluster, HDFSStat.port, use_trash=False)

    def latest_partition(self, table_name, table_path=None):
        t_path = HDFSStat.build_path(table_name) if table_path is None else table_path
        latest_dir = list(self.client.ls([t_path])).pop()
        return path.basename(latest_dir['path']).split('=')[1]

    def poke_partition(self, table_name, partition_name, partition, table_path=None):
        t_path = HDFSStat.build_path(table_name) if table_path is None else table_path
        partition_path = t_path + '/' + partition_name + '=' + partition
        return self.client.test(partition_path, exists=True, directory=True, zero_length=False)
